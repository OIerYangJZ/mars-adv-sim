# GitHub upload

在解压后的 `mosaic-omega` 根目录执行：

```bash
git init
git add .
git commit -m "feat: integrate GoalSpec ToDAG runtime core"
git branch -M main
git remote add origin https://github.com/<YOUR_NAME>/<YOUR_REPO>.git
git push -u origin main
```

第一次推送前建议本地执行：

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest
python scripts/demo_pipeline.py
```

如果 GitHub Actions 变绿，说明仓库基础集成仍然可运行。
