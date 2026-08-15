# GoalSpec Compiler - DeepSeek 语义版

## 目标

把用户自然语言任务转换为标准 GoalSpec JSON，供 DAG 模块读取。

输出六个顶层字段：

- main_goal
- hard_constraints
- soft_preferences
- acceptance_conditions
- budget
- prohibitions

## 两种模式

### 1. rule 模式

本地规则抽取，不需要 API Key。适合作为 fallback。

```powershell
py run_compile.py --mode rule --input examples/input_template_best_case.txt --output test_rule.json
```

### 2. deepseek 模式

DeepSeek 模型语义抽取 + 规则程序复合校验。满足“一个模型抽取，规则程序复合”。

```powershell
$env:DEEPSEEK_API_KEY="你的DeepSeek_API_KEY"
$env:DEEPSEEK_MODEL="deepseek-chat"
py run_compile.py --mode deepseek --input examples/input_zdy_semantic.txt --output test_deepseek.json
```

### 3. auto 模式

优先 DeepSeek，失败自动回退到规则版。

```powershell
py run_compile.py --mode auto --input examples/input_zdy_semantic.txt --output test_auto.json
```

## 安装依赖

```powershell
py -m pip install -r requirements.txt
```

## 最小 API 测试

```powershell
py -c "import os, json; from openai import OpenAI; client=OpenAI(api_key=os.getenv('DEEPSEEK_API_KEY'), base_url='https://api.deepseek.com'); r=client.chat.completions.create(model=os.getenv('DEEPSEEK_MODEL','deepseek-chat'), messages=[{'role':'system','content':'你只输出 json。'}, {'role':'user','content':'输出一个 JSON，字段 ok=true'}], response_format={'type':'json_object'}, max_tokens=100, temperature=0.1); print(r.choices[0].message.content)"
```

## 运行评估

```powershell
py eval_performance.py --mode rule
py eval_performance.py --mode deepseek
```

## 交给 zdy 的文件

- `schema/goalspec.schema.json`
- 生成出来的 `goalspec_for_zdy.json`

