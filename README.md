# 临床实验室质量控制与结果拦截

只使用Python标准库和SQLite的模块化服务，默认端口`8339`。覆盖检测项目、质控品批次、质控规则、允许范围、仪器校准、连续偏差、趋势、失控、结果拦截、复测、调查、批次切换和历史更正。

## 模块结构

- `app.py`：参数解析、依赖组装和服务生命周期。
- `src/domain.py`：角色、领域异常和数据对象。
- `src/rules.py`：QC规则计算、状态机、校准与放行约束。
- `src/repository.py`：SQLite持久化、乐观锁、幂等、事务和审计查询。
- `src/service.py`：用例编排、权限校验、版本控制与对账工作流。
- `src/http_api.py`：JSON接口和统一错误响应。
- `src/audit.py`：操作审计。
- `static/index.html`：最小演示页面。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8339
```

## 核心对象

`assay`为检测项目，`qc_lot`为质控品批次，`instrument`为仪器，`qc_run`为质控结果，`result_batch`为患者结果批次。

## 外部回报对账（reconciliation）

外部回报（`external_report`）按**项目、仪器和批次号**入库，天然键为`assay_id + instrument_id + batch_no`；同一回报重复导入只处理一次，返回原记录。

- `POST /api/external_reports`：导入回报。字段`assay_id`、`instrument_id`、`batch_no`、`reported_value`、`reported_at`。
- `POST /api/entities/<report_id>/actions`，`{"action": "reconcile"}`：把回报与本地质控结果（同项目/仪器最近一次已接受的`qc_run`）及患者结果批次（同批次号）对账。
  - 偏差`|reported_value - 本地值|`不超过`deviation_limit`（可在`assay.rule_config`配置，默认`0.5`）→ 结论`matched`，批次保持`waiting`。
  - 偏差超限 → 结论`deviation`，生成待核异常（`deviation_exception`），并把相关患者批次冻住（`waiting → frozen`）。
  - 找不到已接受的本地质控结果 → 结论`failed`，可重试。
- `POST /api/entities/<reconciliation_id>/actions`，`{"action": "retry"}`：失败后重试，同一对账记录累加`attempt`，不新建记录。
- 待核异常需**两名人员**（`supervisor`/`analyst`）先后`confirm`确认，状态变为`confirmed`后才能`release`放行；放行把冻结批次置为`released`。
- 同一人不能重复确认；确认/放行使用乐观锁，两人同时提交时，后到者收到`409`，响应体含`latest`（最新实体）和`conflicts`（具体冲突项，如版本冲突、已确认、已放行、批次未冻结）。
- 对账幂等：同一回报重复`reconcile`不会重复生成偏差异常、重复冻批次或重复写审计；旧批次冻结/放行后仍可查询。

## 接口

- `GET /health`
- `GET /api/<kind>`，可用`?status=`过滤
- `GET /api/entities/<id>`
- `POST /api/<kind>`
- `POST /api/entities/<id>/actions`
- `GET /api/audit`

身份通过`X-User-Id`和`X-Role`请求头传入。可选`Idempotency-Key`防止重复创建。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 局限

质控规则为可运行的简化模型，包含1-3s、连续偏移和趋势检查，但不替代CLIA、ISO 15189、Westgard完整规则集或实验室信息系统接口。
