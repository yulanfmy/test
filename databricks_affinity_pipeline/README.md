# Affinity データ事業概要自動補完パイプライン

Databricks上で動作する、Affinityから取り込んだスタートアップ企業データの事業概要（`business_description`）カラムを自動補完するパイプラインです。

## アーキテクチャ

```
┌─────────────────────────────────────────────────────────────────┐
│                    Databricks Workspace                         │
│                                                                 │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐  │
│  │  01.抽出  │───▶│ 02.Web   │───▶│ 03.LLM   │───▶│ 04.書き  │  │
│  │  null     │    │  検索    │    │  要約    │    │  戻し    │  │
│  │  レコード │    │          │    │          │    │          │  │
│  └──────────┘    └────┬─────┘    └────┬─────┘    └──────────┘  │
│       │               │               │               │        │
│       ▼               ▼               ▼               ▼        │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │              Unity Catalog (Delta Tables)                │  │
│  │                                                          │  │
│  │  company_master    search_raw_results   enrichment_log   │  │
│  │  (企業マスタ)      (検索生データ)        (処理ログ)       │  │
│  │                    search_normalized    quality_report    │  │
│  │                    (正規化データ)        (品質レポート)    │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │              監査・品質チェックビュー                      │  │
│  │  v_audit_pending_review  v_quality_dashboard              │  │
│  │  v_null_summary_stats                                     │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
│  ┌──────────┐                                                  │
│  │ 05.品質  │──▶ レポート生成 + 監査ビュー更新                 │
│  │ チェック │                                                  │
│  └──────────┘                                                  │
└─────────────────────────────────────────────────────────────────┘

外部API:
  ├── Google Custom Search API / Bing / DuckDuckGo
  └── Databricks Foundation Model API / OpenAI API
```

## パイプライン構成

| ノートブック | 説明 |
|-------------|------|
| `00_setup.py` | カタログ・スキーマ・テーブル・ビューの初期作成 |
| `01_generate_sample_data.py` | ダミーデータ生成（デモ用、7,500社） |
| `02_extract_null_records.py` | null事業概要レコードの抽出・優先順位付け |
| `03_web_search.py` | Web検索API呼び出し・生データ/正規化データ保存 |
| `04_llm_summarize.py` | LLMによる事業概要生成・信頼度スコア算出 |
| `05_write_back.py` | 企業マスタへの書き戻し（高信頼度は自動、低信頼度は保留）|
| `06_quality_check.py` | 品質チェックレポート生成・監査ビュー更新 |
| `07_orchestrator.py` | 全ステップのオーケストレーション（週次ジョブ用）|
| `08_manual_review.py` | 手動レビュー・承認用インタラクティブノートブック |

## テーブル設計

### メインテーブル

#### `company_master` - 企業マスタ
| カラム | 型 | 説明 |
|--------|-----|------|
| company_id | STRING | 企業ID（Affinity由来） |
| company_name | STRING | 企業名 |
| business_description | STRING | 事業概要（**補完対象**） |
| enrichment_status | STRING | 補完ステータス（pending/in_progress/completed/failed/skipped） |
| industry / funding_stage | STRING | 業種・資金調達ステージ |
| ... | | その他メタデータ |

#### `search_raw_results` - 検索生データ
Web検索APIから取得した生の検索結果を保存。

#### `search_normalized` - 正規化検索データ
整形・コンテンツ分類・関連性スコア付きの検索結果。

#### `enrichment_log` - エンリッチメントログ
LLMによる要約生成の処理ログ。信頼度スコア・承認状態を管理。

#### `quality_report` - 品質レポート
バッチ処理ごとの品質メトリクスを記録。

### 監査用ビュー

| ビュー | 用途 |
|--------|------|
| `v_audit_pending_review` | 手動レビュー待ちレコード一覧 |
| `v_quality_dashboard` | 品質ダッシュボード |
| `v_null_summary_stats` | null統計（業種×ステージ別） |

## セットアップ

### 前提条件

- Databricks Workspace（Unity Catalog有効）
- Python 3.10+
- 以下のいずれかの検索API（オプション）:
  - Google Custom Search API キー + Search Engine ID
  - Bing Web Search API キー
  - DuckDuckGo（APIキー不要、デフォルト）

### 1. Databricks Secretsの設定（オプション）

```bash
# Google Search API を使用する場合
databricks secrets create-scope --scope affinity-pipeline
databricks secrets put --scope affinity-pipeline --key google-search-api-key
databricks secrets put --scope affinity-pipeline --key google-search-engine-id

# Bing Search API を使用する場合
databricks secrets put --scope affinity-pipeline --key bing-search-api-key

# OpenAI API を使用する場合（LLMフォールバック）
databricks secrets put --scope affinity-pipeline --key openai-api-key
```

### 2. ノートブックのインポート

```bash
# Databricks CLIを使用
databricks workspace import_dir \
  ./databricks_affinity_pipeline/notebooks \
  /Workspace/databricks_affinity_pipeline/notebooks \
  --overwrite
```

### 3. 初期セットアップ

1. `00_setup.py` を実行 → テーブル・ビューが作成される
2. `01_generate_sample_data.py` を実行 → ダミーデータが生成される

### 4. ワークフロー（ジョブ）の作成

```bash
# Databricks CLIを使用
databricks jobs create --json-file ./databricks_affinity_pipeline/config/workflow_config.json
```

または Databricks UI から手動でジョブを作成し、`07_orchestrator.py` をマスターノートブックとして設定。

## 実行方法

### 手動実行（ノートブック単位）

各ノートブックを順番に実行：
1. `02_extract_null_records.py` → batch_id を取得
2. `03_web_search.py` → batch_id を指定
3. `04_llm_summarize.py` → batch_id を指定
4. `05_write_back.py` → batch_id を指定
5. `06_quality_check.py` → batch_id を指定

### オーケストレーター経由

`07_orchestrator.py` を実行すると、全ステップが自動的に順次実行されます。

### 週次ジョブ

ワークフロー設定（`workflow_config.json`）をインポートすると、毎週月曜日 AM3:00 (JST) に自動実行されます。

## 設定パラメータ

| パラメータ | デフォルト値 | 説明 |
|-----------|-------------|------|
| `max_companies` | 500 | 1回のバッチで処理する最大企業数 |
| `search_provider` | duckduckgo | 検索プロバイダ（google/bing/duckduckgo） |
| `llm_provider` | databricks | LLMプロバイダ（databricks/openai） |
| `llm_model` | databricks-meta-llama-3-1-70b-instruct | LLMモデル名 |
| `confidence_threshold` | 0.7 | 自動承認の信頼度閾値 |
| `auto_approve` | true | 閾値以上の要約を自動承認するか |
| `priority_order` | funding_stage | 処理優先順位（funding_stage/founded_year/random） |

## 品質管理

### 信頼度スコア

各要約に対して0.0〜1.0の信頼度スコアを算出：
- **0.9〜1.0**: 非常に高い信頼度（自動承認）
- **0.7〜0.9**: 高い信頼度（自動承認）
- **0.5〜0.7**: 中程度（手動レビュー推奨）
- **0.0〜0.5**: 低い信頼度（手動レビュー必須）

### 手動レビュー

`08_manual_review.py` ノートブックで：
- 低信頼度の要約を個別にレビュー
- 承認（approve）/ 却下（reject）/ 修正（edit）が可能
- 一括承認機能あり

## ディレクトリ構成

```
databricks_affinity_pipeline/
├── __init__.py
├── config/
│   ├── __init__.py
│   ├── settings.py          # パイプライン設定定数
│   └── workflow_config.json  # Databricksジョブ定義
├── notebooks/
│   ├── 00_setup.py           # 初期セットアップ
│   ├── 01_generate_sample_data.py  # サンプルデータ生成
│   ├── 02_extract_null_records.py  # null抽出
│   ├── 03_web_search.py      # Web検索
│   ├── 04_llm_summarize.py   # LLM要約
│   ├── 05_write_back.py      # 書き戻し
│   ├── 06_quality_check.py   # 品質チェック
│   ├── 07_orchestrator.py    # オーケストレーター
│   └── 08_manual_review.py   # 手動レビュー
└── utils/
    ├── __init__.py
    ├── search_client.py      # Web検索クライアント
    └── llm_client.py         # LLMクライアント
```

## スケール想定

| 項目 | 値 |
|------|-----|
| 対象企業数 | 5,000〜10,000社 |
| 更新頻度 | 週次 |
| 1バッチあたりの処理数 | 最大500社 |
| 全企業の補完完了目安 | 10〜20週 |
| 1バッチの処理時間目安 | 30〜60分 |

## 注意事項

- DuckDuckGo APIは結果が限定的なため、本番運用ではGoogle Custom Search APIまたはBing Search APIの使用を推奨
- LLMのトークン使用量に応じてコストが発生します
- 信頼度閾値を低くするほど自動補完率は上がりますが、品質が低下する可能性があります
- 企業マスタテーブルはChange Data Feed（CDF）が有効化されており、変更履歴の追跡が可能です
