# PR #2 精査済みレビュー (core main 網羅調査版)

**検証方法:** `contract-to-cash/core` の最新 main を網羅的に取得 (eventstore / domain/* / application/tx / application/projection / infrastructure/inmemory の全ファイル)。全インターフェース・エンティティ定義・リファレンス実装を照合。

---

## サマリ

| カテゴリ | 件数 |
|---------|------|
| **Blocker: 設計上の根本問題** | 1 |
| **Critical: コンパイル不可 / 致命的バグ** | 13 |
| High: ロジックバグ | 3 |
| Medium | 5 |
| Low | 6 |

### 結論

**このPRは現状マージ不可能。**

最大の問題は、**「そもそも現在の core の公開APIだけでは PG adapter は実装できない」** という設計上の根本問題。Invoice / CreditNote / Payment / BalanceEntry / Product / Price / UsageRecord の各エンティティは**すべて unexported フィールド + 限定的な setter** しか持たず、再構築用の `Unmarshal` / `Reconstruct` 関数が一切 core に存在しない。

adapter 側で `invoice.Unmarshal(data)` 等を呼んでいる箇所はすべて**未定義関数呼び出しでコンパイル不可**。これを解決するには core に再構築APIを追加するしかない。このPRは本来 core 側のPRと同時に作業すべき。

---

## 🚨 Blocker: 設計上の根本問題

### B1: 非イベントソース型エンティティに再構築APIが一切ない

**影響範囲:** `invoice_repo.go`, `creditnote_repo.go`, `payment_repo.go`, `balance_repo.go`, `usage_repo.go`, `product_repo.go`, `price_repo.go`

adapter は以下の関数/メソッドを呼んでいるが、**core に1つも存在しない:**

| adapter 呼び出し | core の状態 |
|---|---|
| `invoice.Unmarshal(data)` | **未定義** |
| `invoice.UnmarshalCreditNote(data)` | **未定義** |
| `payment.Unmarshal(data)` | **未定義** |
| `balance.UnmarshalEntry(data)` | **未定義** |
| `balance.UnmarshalApplication(data)` | **未定義** |
| `usage.Unmarshal(data)` | **未定義** |
| `product.Unmarshal(data)` | **未定義** |
| `pricing.UnmarshalPrice(data)` | **未定義** |

core 内で `Unmarshal` が存在するのは `Money.UnmarshalJSON`, `DateRange.UnmarshalJSON`, `BillingInterval.UnmarshalJSON` のみ（すべて **JSON 用**）。

さらに問題なのは:
- `Invoice` のフィールドは全て unexported で、`NewInvoice(...)` + functional options では `paidAmount` / `balance` / `voidReason` / `invoiceNumber` の一部状態を復元できない
- `Payment` は `Complete()` / `Fail(reason)` / `RecordRefund(amount)` のメソッド呼び出しを経ないと特定のステータスに到達できない
- `Product` / `Price` のコンストラクタは**新規 ID を生成する** → 既存 ID での再構築不可
- `BalanceEntry.remainingAmount` は `Consume()` メソッド経由でしか変更できない
- `Invoice.Status == Refunded` に至るmutatorが**そもそも存在しない**

**対応:**
1. core に `ReconstructInvoice(...)`, `ReconstructPayment(...)`, `ReconstructBalanceEntry(...)` 等のエクスポート済み再構築関数を追加する（別PR）
2. もしくはドメインエンティティの設計方針を変える
3. それまでこのPRは実装不可能

**このPRを進めるには、まず core 側の設計議論が必要。**

---

## Critical (コンパイル不可 / 致命的バグ)

### C1: `Store.LoadAll` メソッド未実装

**ファイル:** `postgres/eventstore.go`

core の `Store` インターフェースに PR #83 で追加された:
```go
LoadAll(ctx context.Context, fromPosition int64, limit int) ([]Event, error)
```

adapter には非公開 `loadAllFrom` はあるが、公開 `LoadAll` が存在しない。`var _ eventstore.Store = (*PostgresEventStore)(nil)` assertion が通らない。

### C2: `Event.Metadata` の型不一致

**ファイル:** `postgres/eventstore.go:269-275` (scanEvents)

core の `Event.Metadata` は `EventMetadata` struct（`UserID/CorrelationID/CausationID string; IPAddress/UserAgent *string`）。adapter は `json.RawMessage` でスキャンして代入している:

```go
var data, metadata json.RawMessage
...
evt.Metadata = metadata  // コンパイルエラー
```

**対応:** `json.Unmarshal(metadata, &evt.Metadata)` する。ただし DB の `metadata` カラムは JSONB なので SQL 側でそのまま `EventMetadata` にScan できるかは要検証。

### C3: `contract.NewEmptyAggregate` は存在しない

**ファイル:** `postgres/contract_repo.go:64, 145`

core のコンストラクタは以下のみ:
```go
func NewContractAggregate(id shared.ContractID, clock shared.Clock) *ContractAggregate
```

**対応:** `NewContractAggregate(id, r.clock)` を使う。`PostgresContractRepository` に `shared.Clock` フィールドを追加。

### C4: エラー返却体系がすべて誤り

**ファイル:** 全リポジトリファイル、`eventstore.go`

**adapter が使っているが存在しないシンボル:**
- `eventstore.ErrConcurrencyConflict`
- `contract.ErrNotFound`
- `invoice.ErrNotFound`
- `invoice.ErrCreditNoteNotFound`
- `payment.ErrNotFound`
- `balance.ErrNotFound`
- `product.ErrNotFound`
- `pricing.ErrNotFound`

core での実際のエラーパターン:
```go
// NotFound
return nil, shared.NewDomainError(shared.ErrCodeNotFound,
    fmt.Sprintf("invoice %s not found", id))

// VersionConflict - inmemory の実装は2通りに分かれる
// 1. EventStore系: shared.NewDomainError(shared.ErrCodeVersionConflict, ...)
// 2. BalanceRepo系: tx.ErrVersionConflict (直接返す)
```

**重要:** `tx.RetryOnConflict` は `errors.Is(err, tx.ErrVersionConflict)` で判定するため、リトライ動作を期待するリポジトリは **`tx.ErrVersionConflict` を直接返す**必要がある。

**core 側の inconsistency:** `InMemoryEventStore.Append` は `shared.NewDomainError(ErrCodeVersionConflict)` を返しているため、`RetryOnConflict` が動かない可能性がある（core 側のバグの疑い）。PG adapter は `tx.ErrVersionConflict` を返すべき。

### C5: `shared.NewMoney(int64, Currency)` シグネチャ不一致

**ファイル:** `postgres/balance_repo.go:114`

core のシグネチャ: `func NewMoney(amount *big.Rat, currency Currency) Money`

**対応:** `new(big.Rat).SetInt64(n)` でラップするか、`shared.Zero(currency)` から `Add` で積む。`inmemory/balance_repository.go` の `GetBalance` を参考に:
```go
total := shared.Zero(currency)
total, err = total.Add(entry.RemainingAmount())
```

### C6: `DateRange.From` / `DateRange.To` は存在しない

**ファイル:** `postgres/invoice_repo.go:186-187`, `postgres/usage_repo.go:64-65`

core の `DateRange` は unexported フィールド `start/end` + メソッド `Start()/End()` のみ。

**対応:** `period.Start()`, `period.End()` に変更。

### C7: `BalanceEntry.Amount()` / `BalanceEntry.Currency()` は存在しない

**ファイル:** `postgres/balance_repo.go:44-47`

core の `BalanceEntry` に存在するメソッド:
- `OriginalAmount() shared.Money`
- `RemainingAmount() shared.Money`
- `Currency` は `OriginalAmount().Currency()` 経由

`Amount()` や `Currency()` メソッドは**存在しない**。adapter のコードはコンパイルエラー。

### C8: `balance_entries` テーブルのスキーマが `Original/Remaining` を表現できない

**ファイル:** `postgres/migrations/003_read_models.up.sql`

```sql
CREATE TABLE balance_entries (
    id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    amount BIGINT NOT NULL CHECK (amount > 0),  -- ← original/remaining の区別なし
    ...
)
```

必要なカラム:
- `original_amount BIGINT`
- `remaining_amount BIGINT`
- `reason TEXT`
- `source_type TEXT`
- `source_id TEXT`
- `description TEXT`
- `expires_at TIMESTAMPTZ`
- `version INTEGER` — **楽観的ロック用**
- `currency TEXT`

### C9: `balance.BalanceEntry` の楽観的ロックが未実装

**ファイル:** `postgres/balance_repo.go:31-52`

core の `BalanceEntry` には `Version()`, `LoadedVersion()`, `SetVersion()` があり、inmemory 実装では:
```go
if storedVersion, ok := r.versions[entry.ID()]; ok {
    if entry.LoadedVersion() != storedVersion {
        return tx.ErrVersionConflict
    }
}
```

adapter の `Save` は `ON CONFLICT DO UPDATE` で無条件上書き。PR description に「optimistic locking (LoadedVersion)」と書いてあるが実装されていない。

### C10: `BalanceApplication` / `BalanceRefund` はメソッドではなく **public フィールド**

**ファイル:** `postgres/balance_repo.go:120-135, 175-185`

core の定義:
```go
type BalanceApplication struct {
    ID             string
    BalanceEntryID shared.BalanceEntryID
    InvoiceID      shared.InvoiceID
    Amount         shared.Money
    AppliedAt      time.Time
}
```

adapter は `app.ID()`, `app.BalanceEntryID()`, `app.InvoiceID()`, `app.Amount()` とメソッド呼び出しをしているが、**これらはフィールドでありメソッドではない**。`.` で直接アクセスすべき:
```go
app.ID, app.BalanceEntryID, app.InvoiceID, app.Amount
```

同様に `BalanceRefund` も:
```go
type BalanceRefund struct {
    ID             string
    BalanceEntryID shared.BalanceEntryID
    AccountID      shared.AccountID
    Amount         shared.Money
    RefundedAt     time.Time
}
```

adapter の `refund.ID()`, `refund.BalanceEntryID()`, `refund.Amount()` もコンパイルエラー。

### C11: `usage.NewSummary(...)` は存在しない

**ファイル:** `postgres/usage_repo.go:69`

```go
return usage.NewSummary(contractID, metric, period, 0, 0), nil
```

core の `UsageSummary` は **public フィールド**の struct で、コンストラクタなし:
```go
type UsageSummary struct {
    ContractID  shared.ContractID
    MetricName  shared.MetricName
    Period      shared.DateRange
    TotalUsage  int64
}
```

**対応:** 構造体リテラルで構築:
```go
return &usage.UsageSummary{
    ContractID: contractID,
    MetricName: metric,
    Period:     period,
    TotalUsage: totalQuantity,
}, nil
```

### C12: `ContractAggregate.Version()` の使い方が誤り

**ファイル:** `postgres/contract_repo.go:45`

```go
if err := es.Append(ctx, streamID, changes, aggregate.Version()-len(changes)); err != nil {
```

core の `BaseAggregate.Version()` は**永続化済みバージョン**を返す（UncommittedEvents を含まない）。inmemory 実装の正しいパターン:
```go
expectedVersion := aggregate.Version()
if err := r.store.Append(ctx, aggregate.ID(), events, expectedVersion); err != nil {
    return err
}
aggregate.SetVersion(aggregate.Version() + len(events))
```

adapter は `Version()-len(changes)` で負の値を計算してしまう。また Save 後の `SetVersion` 呼び出しも欠落しており、連続 Save でバージョンが進まない。

### C13: streamID 規約の不一致

**ファイル:** `postgres/contract_repo.go:212-214`

adapter は `"contract-" + id` を streamID とする。一方 core の inmemory 実装は `aggregate.ID()` をそのまま streamID とする（プレフィックスなし）。

`ContractProjector.isContractEvent` も `strings.HasPrefix(evt.StreamID, "contract-")` でフィルタするため、inmemory と互換性がない。core の規約を明確化する必要がある。

（注: core の `NewContractAggregate(id, clock)` は `NewBaseAggregate(string(id), clock)` を呼んでおり、bare ContractID が stream ID になる設計と読める。**プレフィックスを削除するのが正しい。**）

---

## High (ロジックバグ / データ整合性)

### H1: EventStore.Append のUNIQUE制約違反が `tx.ErrVersionConflict` に変換されていない

**ファイル:** `postgres/eventstore.go:47-80`

`FOR UPDATE` + `MAX(version)` → INSERT のパターンだが、UNIQUE(stream_id, version) 違反時に生 `pgconn.PgError` が返る。`RetryOnConflict` が動かない。

**対応:** INSERT 失敗を検出して:
```go
var pgErr *pgconn.PgError
if errors.As(err, &pgErr) && pgErr.Code == "23505" {
    return tx.ErrVersionConflict
}
```

### H2: Invoice.Save の3-step SQL がトランザクション外で不整合リスク

**ファイル:** `postgres/invoice_repo.go:40-72`

UPSERT → history close → history insert の3ステップが、Pool直接実行の場合にアトミック性を欠く。

### H3: `FindAvailable` の判定が inmemory と乖離

**ファイル:** `postgres/balance_repo.go:75-88`

inmemory: `IsExpired(now)` / `IsFullyConsumed()` / `RemainingAmount` の各チェック。  
adapter: `amount > SUM(applications)` のみ。expired / fully consumed / refunds が考慮されない。

---

## Medium

### M1: InvoiceProjector.Rebuild がTxチェックなし

### M2: NOTIFY の N回発火 (`FOR EACH ROW` → `FOR EACH STATEMENT` に)

### M3: go.sum なし、replace ディレクティブなし (プライベート module の可能性)

### M4: currency CHECK制約が `payments` / `prices` / `balance_entries` で欠落

### M5: ContractRepository.Save で `SetVersion` 呼び出し抜け（C12と関連）

---

## Low

### L1: テスト0件

### L2: `loadAggregatesByRows` の N+1 問題

### L3: `contractStreamID` の型一貫性

### L4: down migration なし

### L5: Read model の `data` カラム意味不明

### L6: LISTEN/NOTIFY チャネル名の sanitize (現状定数のため実害なし)

### L7: `product.NewProduct` / `pricing.NewPrice` は新規ID生成 → 再構築時の ID 保持が問題 (B1 と関連)

### L8: `CheckpointStore` は core の `application/projection` にない新規概念 — adapter 独自で良いが、上流化も検討

---

## 取得できた core の重要事実（検証根拠）

### 1. `eventstore.Store` インターフェース完全版 (10メソッド)
```go
type Store interface {
    Append(ctx, streamID, events, expectedVersion) error
    Load(ctx, streamID) ([]Event, error)
    LoadUntilVersion(ctx, streamID, version) ([]Event, error)
    LoadUntil(ctx, streamID, until) ([]Event, error)
    LoadRange(ctx, streamID, from, to) ([]Event, error)
    LoadAll(ctx, fromPosition, limit) ([]Event, error)  // ← PR #83 で追加
    Subscribe(ctx, fromPosition) (<-chan Event, error)
    SaveSnapshot(ctx, snapshot) error
    LoadSnapshot(ctx, streamID) (*Snapshot, error)
    LoadSnapshotBefore(ctx, streamID, before) (*Snapshot, error)
}
```

### 2. `Event` 構造体
```go
type Event struct {
    ID, StreamID string
    Type EventType
    Version, SchemaVersion int
    Data json.RawMessage
    Metadata EventMetadata  // ← struct、json.RawMessage ではない
    OccurredAt, RecordedAt time.Time
    GlobalPosition int64  // ← PR #83 で追加済み
}
```

### 3. エラー定義の全体像
- core の **sentinel errors は `tx.ErrVersionConflict` と `service.ErrRequiresAction` の2つだけ**
- 他は全て `shared.DomainError` + `ErrCode*` パターン
- inmemory の実装でも `VersionConflict` の返却方法が2通りに分かれている（バグの疑い）

### 4. `application/projection.Projector` インターフェース
```go
type Projector interface {
    Project(ctx, event) error
    Rebuild(ctx, until) error
}
```
→ adapter の実装はこのシグネチャと一致する。**C1 以外の projector 関連は問題なし。**  
ただし `CheckpointStore` は core にないため adapter 独自定義。

### 5. `BalanceApplication` / `BalanceRefund` は public フィールド
→ メソッド呼び出しは全てコンパイルエラー。

### 6. `UsageSummary` も public フィールド、コンストラクタなし

### 7. 非イベントソース型のエンティティ復元手段は一切ない
→ **B1 (Blocker)**

---

## 推奨アクション

1. **まず core 側で対応PRを立てる**
   - 各エンティティに `ReconstructXxx(...)` コンストラクタを追加
   - `tx.ErrVersionConflict` / `shared.ErrCodeVersionConflict` の使い分けを統一
   - `EventStore` の streamID 規約をドキュメント化
   
2. **本PRは close して作り直し**
   - core の inmemory 実装を隣に置いて、各メソッドを1対1で移植する
   - 未実装の LoadAll を追加
   - エラー返却を `shared.NewDomainError` / `tx.ErrVersionConflict` に統一
   - BalanceEntry のスキーマを再設計（original/remaining/version/expires_at を追加）
   - BalanceApplication/Refund/UsageSummary のフィールドアクセスに修正
   - Contract aggregate の Version 管理を inmemory と同じパターンに
   
3. **テストを追加する**
   - 少なくとも以下の統合テスト:
     - EventStore.Append の optimistic locking（並行Tx で conflict → `errors.Is(err, tx.ErrVersionConflict)`）
     - Contract aggregate の snapshot + replay round-trip
     - Invoice.Save の temporal history
     - Balance の LoadedVersion による optimistic locking

---

## 変更履歴

- 初版: adapter ソース単独のレビュー (High/Medium 中心の指摘)
- 第2版: core の主要インターフェースと照合 → Critical 7件発見
- **第3版 (本版): core main を網羅的に調査、inmemory 実装と照合、Blocker 問題発見**

adapter が前提としている public API の大半が core に存在しないことが判明。このPRは**実装方針そのものを再設計する必要がある**。
