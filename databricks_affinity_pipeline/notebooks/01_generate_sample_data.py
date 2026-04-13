# Databricks notebook source
# MAGIC %md
# MAGIC # 01. サンプルデータ生成
# MAGIC
# MAGIC Affinityから取り込んだ想定のスタートアップ企業ダミーデータを生成します。
# MAGIC
# MAGIC - 生成件数: 5,000〜10,000社
# MAGIC - 事業概要（business_description）の約40%をnullに設定
# MAGIC - 日本のスタートアップ企業を想定したリアルなダミーデータ

# COMMAND ----------

import random
import uuid
from datetime import datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. データ生成パラメータ

# COMMAND ----------

# dbutils.widgets でノートブックパラメータを定義
dbutils.widgets.text("num_companies", "7500", "生成する企業数")
dbutils.widgets.dropdown("null_rate", "0.4", ["0.2", "0.3", "0.4", "0.5", "0.6"], "null比率")

NUM_COMPANIES = int(dbutils.widgets.get("num_companies"))
NULL_RATE = float(dbutils.widgets.get("null_rate"))

print(f"Generating {NUM_COMPANIES} companies with {NULL_RATE*100:.0f}% null descriptions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. ダミーデータ定義

# COMMAND ----------

# 業種マスタ
INDUSTRIES = {
    "SaaS": ["HR Tech", "FinTech", "EdTech", "MarTech", "LegalTech", "PropTech", "InsurTech"],
    "AI/ML": ["画像認識", "自然言語処理", "予測分析", "生成AI", "ロボティクス"],
    "ヘルスケア": ["デジタルヘルス", "創薬", "医療機器", "遠隔医療", "ヘルスデータ"],
    "フィンテック": ["決済", "融資", "資産運用", "暗号資産", "保険"],
    "EC/リテール": ["D2C", "マーケットプレイス", "物流テック", "リテールテック"],
    "モビリティ": ["EV", "自動運転", "MaaS", "ドローン", "宇宙"],
    "環境/エネルギー": ["クリーンテック", "再エネ", "カーボンテック", "サーキュラーエコノミー"],
    "不動産テック": ["不動産管理", "スマートビル", "建設テック"],
    "教育": ["オンライン教育", "企業研修", "語学学習", "STEM教育"],
    "エンタメ": ["ゲーム", "動画配信", "音楽テック", "メタバース", "NFT"],
}

FUNDING_STAGES = [
    "Pre-Seed", "Seed", "Series A", "Series B", "Series C",
    "Series D+", "IPO準備中", "Bootstrap",
]

HEADQUARTERS = [
    "東京都渋谷区", "東京都港区", "東京都千代田区", "東京都新宿区", "東京都品川区",
    "東京都中央区", "東京都文京区", "大阪府大阪市", "愛知県名古屋市", "福岡県福岡市",
    "京都府京都市", "神奈川県横浜市", "北海道札幌市", "宮城県仙台市", "広島県広島市",
    "兵庫県神戸市", "沖縄県那覇市", "茨城県つくば市",
]

# 企業名生成用パーツ
NAME_PREFIXES = [
    "スマート", "テック", "グロース", "イノベート", "フューチャー", "ネクスト",
    "デジタル", "クラウド", "アクセル", "グローバル", "サイバー", "データ",
    "エッジ", "プライム", "コア", "ブリッジ", "シナジー", "ピボット",
    "リンク", "ハブ", "フロー", "ビット", "ゼロ", "ワン",
]

NAME_SUFFIXES = [
    "ラボ", "テクノロジーズ", "ソリューションズ", "プラットフォーム", "AI",
    "システムズ", "ネットワーク", "ベンチャーズ", "クリエイト", "ワークス",
    "イノベーション", "デザイン", "コンサルティング", "ファクトリー", "スタジオ",
    "パートナーズ", "ジャパン", "グループ", "ホールディングス", "リサーチ",
]

# 事業概要テンプレート
DESCRIPTION_TEMPLATES = [
    "{industry}領域において、{product}を提供するスタートアップ。{target}向けに{value}を実現し、{metric}の改善を支援する。",
    "{product}を開発・提供する{industry}スタートアップ。主に{target}を対象に、{value}を通じて{metric}の向上に貢献。",
    "{target}の{pain}を解決するため、{product}を展開。{industry}分野で{value}を追求し、{metric}の最適化を図る。",
    "独自の{tech}技術を活用した{product}を開発。{industry}における{target}の{pain}を解消し、{metric}を大幅に改善。",
    "{industry}特化型の{product}プラットフォームを運営。{target}に対して{value}を提供し、業界の{pain}の解決に取り組む。",
]

PRODUCTS = [
    "クラウドベースの業務管理SaaS", "AIを活用した分析プラットフォーム",
    "ブロックチェーンベースの管理システム", "IoTデバイスとデータ分析基盤",
    "モバイルファーストのアプリケーション", "API連携プラットフォーム",
    "ノーコード/ローコード開発ツール", "リアルタイムデータ処理エンジン",
    "自動化ワークフローツール", "統合データプラットフォーム",
]

TARGETS = [
    "中小企業", "大企業", "スタートアップ", "個人事業主",
    "医療機関", "教育機関", "金融機関", "製造業",
    "小売業", "不動産業界",
]

VALUES = [
    "業務効率化", "コスト削減", "意思決定の迅速化", "顧客体験の向上",
    "データ活用の促進", "DX推進", "セキュリティ強化", "収益最大化",
]

PAINS = [
    "非効率な業務プロセス", "データのサイロ化", "人手不足",
    "レガシーシステムの課題", "コミュニケーション課題",
    "コスト高騰", "セキュリティリスク",
]

METRICS = ["生産性", "売上", "コスト効率", "顧客満足度", "業務効率", "ROI"]

TECHS = ["AI/機械学習", "ブロックチェーン", "IoT", "エッジコンピューティング", "量子コンピューティング"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. データ生成関数

# COMMAND ----------

def generate_company_name(idx: int) -> tuple[str, str]:
    """企業名と英語名を生成"""
    prefix = random.choice(NAME_PREFIXES)
    suffix = random.choice(NAME_SUFFIXES)
    jp_name = f"{prefix}{suffix}"
    en_name = f"{prefix}{suffix} Inc."
    return jp_name, en_name


def generate_description(industry: str, sub_industry: str) -> str:
    """事業概要を生成"""
    template = random.choice(DESCRIPTION_TEMPLATES)
    return template.format(
        industry=sub_industry or industry,
        product=random.choice(PRODUCTS),
        target=random.choice(TARGETS),
        value=random.choice(VALUES),
        pain=random.choice(PAINS),
        metric=random.choice(METRICS),
        tech=random.choice(TECHS),
    )


def generate_website(name_en: str) -> str:
    """Webサイト URLを生成"""
    domain = name_en.lower().replace(" inc.", "").replace(" ", "").replace("/", "")
    return f"https://www.{domain}.co.jp"


def generate_companies(num: int, null_rate: float) -> list[dict]:
    """企業データを生成"""
    companies = []
    random.seed(42)

    for i in range(num):
        industry = random.choice(list(INDUSTRIES.keys()))
        sub_industry = random.choice(INDUSTRIES[industry])
        jp_name, en_name = generate_company_name(i)

        # 重複回避のためにインデックスを付与
        jp_name = f"{jp_name}{i+1:05d}"
        en_name = en_name.replace(" Inc.", f"{i+1:05d} Inc.")

        # null_rateに基づいて事業概要をnullにする
        has_description = random.random() > null_rate
        description = generate_description(industry, sub_industry) if has_description else None

        founded = random.randint(2010, 2025)
        base_date = datetime(2025, 1, 1)
        created_at = base_date - timedelta(days=random.randint(0, 365))
        affinity_updated = created_at + timedelta(days=random.randint(0, 30))

        company = {
            "company_id": f"aff_{uuid.uuid4().hex[:12]}",
            "company_name": jp_name,
            "company_name_en": en_name,
            "website_url": generate_website(en_name),
            "industry": industry,
            "sub_industry": sub_industry,
            "founded_year": founded,
            "headquarters": random.choice(HEADQUARTERS),
            "employee_count": random.choice([None, *range(1, 500)]),
            "funding_stage": random.choice(FUNDING_STAGES),
            "total_funding_jpy": random.choice(
                [None, *[random.randint(1_000_000, 10_000_000_000) for _ in range(5)]]
            ),
            "business_description": description,
            "affinity_list_id": f"list_{random.randint(1000, 9999)}",
            "affinity_updated_at": affinity_updated.isoformat(),
            "created_at": created_at.isoformat(),
            "updated_at": None,
            "enrichment_status": "pending" if description is None else "skipped",
        }
        companies.append(company)

    return companies

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. データ生成・テーブルへの書き込み

# COMMAND ----------

# データ生成
companies = generate_companies(NUM_COMPANIES, NULL_RATE)

print(f"Generated {len(companies)} companies")
null_count = sum(1 for c in companies if c["business_description"] is None)
print(f"  - With description: {len(companies) - null_count}")
print(f"  - Without description (null): {null_count}")
print(f"  - Null rate: {null_count / len(companies) * 100:.1f}%")

# COMMAND ----------

# DataFrameに変換
from pyspark.sql import Row
from pyspark.sql.functions import to_timestamp

df = spark.createDataFrame([Row(**c) for c in companies])

# タイムスタンプ型に変換
df = (
    df.withColumn("affinity_updated_at", to_timestamp("affinity_updated_at"))
    .withColumn("created_at", to_timestamp("created_at"))
)

# テーブルに書き込み（既存データは上書き）
df.write.mode("overwrite").saveAsTable("affinity_demo.company_enrichment.company_master")

print(f"Wrote {df.count()} records to company_master")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. データ確認

# COMMAND ----------

# 全体統計
display(spark.sql("""
SELECT
    COUNT(*) AS total_companies,
    SUM(CASE WHEN business_description IS NULL THEN 1 ELSE 0 END) AS null_descriptions,
    ROUND(SUM(CASE WHEN business_description IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS null_rate_pct,
    COUNT(DISTINCT industry) AS num_industries,
    COUNT(DISTINCT funding_stage) AS num_funding_stages
FROM affinity_demo.company_enrichment.company_master
"""))

# COMMAND ----------

# 業種別統計
display(spark.sql("""
SELECT
    industry,
    COUNT(*) AS count,
    SUM(CASE WHEN business_description IS NULL THEN 1 ELSE 0 END) AS null_count,
    ROUND(SUM(CASE WHEN business_description IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS null_rate
FROM affinity_demo.company_enrichment.company_master
GROUP BY industry
ORDER BY count DESC
"""))

# COMMAND ----------

# サンプルデータ表示
display(spark.sql("""
SELECT company_id, company_name, industry, funding_stage, business_description, enrichment_status
FROM affinity_demo.company_enrichment.company_master
LIMIT 20
"""))
