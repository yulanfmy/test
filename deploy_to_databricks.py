#!/usr/bin/env python3
"""
Databricks Workspace へのデプロイスクリプト

Usage:
    python deploy_to_databricks.py --workspace-url https://xxx.cloud.databricks.com --token <PAT>

このスクリプトは以下を実行します:
1. ノートブックを Databricks Workspace にアップロード
2. Secrets Scope の作成（必要に応じて）
3. ワークフロー（ジョブ）の作成
"""

import argparse
import base64
import json
from pathlib import Path

import requests


class DatabricksDeployer:
    """Databricks Workspace へのデプロイを管理"""

    def __init__(self, workspace_url: str, token: str):
        self.base_url = workspace_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update(
            {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        )

    def _api(self, method: str, path: str, **kwargs) -> dict:
        url = f"{self.base_url}/api/2.0{path}"
        resp = self.session.request(method, url, **kwargs)
        resp.raise_for_status()
        return resp.json() if resp.content else {}

    # ── Workspace ──

    def mkdir(self, workspace_path: str) -> None:
        """Workspace上にディレクトリを作成"""
        self._api("POST", "/workspace/mkdirs", json={"path": workspace_path})
        print(f"  Created directory: {workspace_path}")

    def upload_notebook(self, local_path: str, workspace_path: str) -> None:
        """ノートブックをアップロード"""
        content = Path(local_path).read_text(encoding="utf-8")
        encoded = base64.standard_b64encode(content.encode("utf-8")).decode("utf-8")
        self._api(
            "POST",
            "/workspace/import",
            json={
                "path": workspace_path,
                "language": "PYTHON",
                "content": encoded,
                "overwrite": True,
                "format": "SOURCE",
            },
        )
        print(f"  Uploaded: {local_path} -> {workspace_path}")

    def upload_notebooks(self, local_dir: str, workspace_dir: str) -> None:
        """ディレクトリ内のノートブックを一括アップロード"""
        self.mkdir(workspace_dir)
        for f in sorted(Path(local_dir).glob("*.py")):
            name = f.stem
            ws_path = f"{workspace_dir}/{name}"
            self.upload_notebook(str(f), ws_path)

    # ── Secrets ──

    def create_secret_scope(self, scope_name: str) -> None:
        """Secret Scopeの作成"""
        try:
            self._api(
                "POST",
                "/secrets/scopes/create",
                json={"scope": scope_name, "initial_manage_principal": "users"},
            )
            print(f"  Created secret scope: {scope_name}")
        except requests.HTTPError as e:
            if e.response.status_code == 409:
                print(f"  Secret scope already exists: {scope_name}")
            else:
                raise

    # ── Jobs ──

    def create_job(self, config_path: str) -> int:
        """ジョブを作成"""
        config = json.loads(Path(config_path).read_text(encoding="utf-8"))
        result = self._api("POST", "/jobs/create", json=config)
        job_id = result.get("job_id")
        print(f"  Created job: {config.get('name', 'unknown')} (ID: {job_id})")
        return job_id

    def list_jobs(self, name_filter: str = "") -> list[dict]:
        """ジョブの一覧取得"""
        result = self._api("GET", "/jobs/list", params={"name": name_filter} if name_filter else {})
        return result.get("jobs", [])


def main():
    parser = argparse.ArgumentParser(description="Deploy to Databricks Workspace")
    parser.add_argument("--workspace-url", required=True, help="Databricks workspace URL")
    parser.add_argument("--token", required=True, help="Databricks PAT (Personal Access Token)")
    parser.add_argument(
        "--workspace-path",
        default="/Workspace/databricks_affinity_pipeline",
        help="Workspace上のデプロイ先パス",
    )
    parser.add_argument(
        "--skip-job", action="store_true", help="ジョブの作成をスキップ"
    )
    parser.add_argument(
        "--skip-secrets", action="store_true", help="Secret Scopeの作成をスキップ"
    )
    args = parser.parse_args()

    deployer = DatabricksDeployer(args.workspace_url, args.token)
    project_dir = Path(__file__).parent / "databricks_affinity_pipeline"

    print("\n=== Deploying to Databricks Workspace ===")
    print(f"Workspace URL: {args.workspace_url}")
    print(f"Target path: {args.workspace_path}")

    # 1. ノートブックのアップロード
    print("\n[1/3] Uploading notebooks...")
    notebooks_dir = project_dir / "notebooks"
    deployer.upload_notebooks(str(notebooks_dir), f"{args.workspace_path}/notebooks")

    # 2. Secret Scopeの作成
    if not args.skip_secrets:
        print("\n[2/3] Creating secret scope...")
        deployer.create_secret_scope("affinity-pipeline")
        print("  Note: Please set the following secrets manually:")
        print("    - google-search-api-key (optional)")
        print("    - google-search-engine-id (optional)")
        print("    - bing-search-api-key (optional)")
        print("    - openai-api-key (optional)")
    else:
        print("\n[2/3] Skipping secret scope creation")

    # 3. ジョブの作成
    if not args.skip_job:
        print("\n[3/3] Creating workflow job...")
        config_path = project_dir / "config" / "workflow_config.json"
        job_id = deployer.create_job(str(config_path))
        print(f"  Job URL: {args.workspace_url}/#job/{job_id}")
    else:
        print("\n[3/3] Skipping job creation")

    print("\n=== Deployment complete! ===")
    print("\nNext steps:")
    print("  1. Run 00_setup notebook to create tables")
    print("  2. Run 01_generate_sample_data notebook to generate demo data")
    print("  3. Run 07_orchestrator or start the scheduled job")


if __name__ == "__main__":
    main()
