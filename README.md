# ChatHygiene

中文 | [English](README_en.md)

## 介绍

ChatHygiene 是一个自托管 Rust 服务。它通过已连接的 Telegram Business
机器人验证陌生私信发件人，并在明确启用后删除高置信度垃圾消息。每个部署只服务
一个 Telegram 账号；普通消息正文不会被有意写入数据库，显式标注的训练样本除外。

Telegram 会先把消息交给用户，再把更新发送给机器人，因此通知或聊天列表可能在
ChatHygiene 处理前短暂显示消息。检测、权限或 Telegram 调用失败时，服务默认保留
消息。

## 功能

- 陌生联系人收到一道人机算术题；所有者手动回复后，该对话进入 `ACTIVE`。
- 本地确定性规则检查文本、说明和 Telegram 实体，不下载附件，不调用 LLM。
- 只有分数为 `100` 的 `SPAM` 才可能触发删除；`SUSPICIOUS` 始终保留。
- 首次启动默认 dry-run，不自动标记已读、删除或软屏蔽。
- SQLite 恢复已记录事件、挑战、连接协调状态和发件箱操作。
- 安装主种子、Owner 身份、bot ID pin、候选连接与可信连接均由数据库管理。
- 应用每次启动都会自动协调 Telegram webhook，不要求运维人员生成应用密钥。

## 工作原理

```text
陌生私信 -> 本地检测 -> 安全消息发起算术验证
                    -> 高置信度垃圾消息进入清理候选
所有者手动回复 -> ACTIVE，不再检测该对话
```

算术题有效期为两分钟，允许三次数字答案；非数字消息不消耗次数。答案正确后对话
进入 `VERIFIED_WAITING_OWNER`。所有者回复后进入 `ACTIVE`，直到已观察到的所有者
回复都被删除，或所有者执行 `/reset <chat_id>`。

判定边界：

| 分数 | 判定 | 行为 |
| --- | --- | --- |
| 0–49 | `ALLOW` | 保留消息并继续生命周期。 |
| 50–99 | `SUSPICIOUS` | 保留消息供所有者检查。 |
| 100 | `SPAM` | 记录证据；仅在 dry-run 关闭时执行清理。 |

## 部署

### 前提

- Linux、Docker Engine 和 Docker Compose v2；
- BotFather 创建并启用 Business/Secretary 支持的机器人；
- 一个公开 HTTPS URL；
- 一个负责 TLS 终止并持续转发 `/telegram/webhook` 的反向代理、隧道或网关；
- 受保护的部署账号、`.env` 和数据目录。

应用在容器内固定监听明文 HTTP `8080`。Telegram 接受的公开 webhook 端口只有
`443`、`80`、`88` 和 `8443`，ChatHygiene 对这四个端口仍一律要求 HTTPS。推荐公开
`443` 终止 TLS，再转发到容器 `8080`。`https://host:8080/...` 会在本地配置阶段被
拒绝；Docker 端口映射本身不提供 TLS。

默认公开 URL 为：

```dotenv
CHATHYGIENE_PUBLIC_WEBHOOK_URL=https://host/telegram/webhook
```

反向代理必须把该公开路径转发到容器相同的 `/telegram/webhook`，并保留
`X-Telegram-Bot-Api-Secret-Token`。例如：

```nginx
location = /telegram/webhook {
    proxy_pass http://127.0.0.1:8080/telegram/webhook;
    proxy_set_header X-Telegram-Bot-Api-Secret-Token $http_x_telegram_bot_api_secret_token;
}
```

也可以配置不同的公开路径或 query，但必须同时配置到内部固定路由的对应重写。
Telegram 接受 webhook 声明并不证明代理不会返回 `404`/`403`，也不证明密钥 header
被保留。

### 全新部署

全新安装只需要 bot token、公开 webhook URL、受保护的数据目录和 dry-run 默认值。
在第一次 `docker compose up` 前显式创建 bind source；不要让 Compose 自动创建通常为
`0755` 的宿主目录，因为容器入口会拒绝它。

```bash
install -d -m 0700 data
cp .env.example .env
chmod 600 .env
stat -c '%a %n' data .env
```

如果部署账号不是当前账号，停止后对这两个精确路径执行所需的 `chown`，不要递归
修改未知目录。预期 `data` 为 `700`，`.env` 为 `600`，且均属于部署账号。

填写 `.env`：

```dotenv
CHATHYGIENE_BOT_TOKEN=BotFather签发的令牌
CHATHYGIENE_PUBLIC_WEBHOOK_URL=https://host/telegram/webhook
CHATHYGIENE_DESTRUCTIVE_MODE=false
CHATHYGIENE_PORT=8080
RUST_LOG=info
```

`CHATHYGIENE_PORT` 只是宿主机到容器固定 `8080` 的映射，不是任意 Telegram 公开端口。

导入与 VPS 架构匹配的 Release 镜像并启动：

```bash
uname -m
docker load -i chathygiene-amd64-YYYY.MM.DD.tar
docker compose up -d
docker compose ps
curl --fail http://127.0.0.1:8080/health/live
curl --fail http://127.0.0.1:8080/health/ready
```

稳定的新机器人顺序是：

1. 在 BotFather 创建并配置机器人。
2. 用 bot token 和公开 URL 部署并启动 ChatHygiene。
3. 等待 HTTP readiness 与自动 webhook 协调完成。
4. 在 Telegram Business 中把机器人绑定一次，并授予回复、已读、删除已发送消息、
   删除收到/全部消息四项权限。
5. 在普通机器人私聊中点击 Start。
6. 读取准备好的完整命令：

   ```bash
   docker compose exec -T chathygiene cat /data/claim-code
   ```

   仅当宿主所有权允许时，也可运行 `cat data/claim-code`。
7. 在普通私聊中发送该命令，然后检查 Owner、连接和 dry-run 状态。

`/claim` 从 Telegram 已认证的 `message.from.id` 读取 Owner 用户 ID，从
`message.chat.id` 读取投递 chat ID；`CHATHYGIENE_OWNER_USER_ID` 已不是有效配置。

### Owner claim 与 `/start`

未 claim 时，ChatHygiene 在 SQLite 数据库旁自动创建 `claim-code`，权限为 `0600`。
它包含完整 `/claim <token>` 命令。未 claim 的同一数据库重启后，如果文件丢失，会
重新创建同一命令；claim 成功后只删除内容、类型、所有权都完全安全匹配的副本。
不安全、symlink、错误所有者或内容不匹配的条目不会被自动删除，必须由运维人员停机
修复。

该文件不是应用 CLI，不需要单独备份。删除文件也不是撤销：claim 前数据库备份包含
可派生同一 token 的主种子。发送成功后，请删除 Telegram 中的 `/claim` 消息；
Telegram 历史、通知、终端输出和剪贴板不在 ChatHygiene 的数据库/日志脱敏边界内。

`/start` 永远不会发送 claim code，也不会制造 `business_connection` 更新。claim 前
它只提示读取同级文件；claim 后只向 Owner 显示帮助，非 Owner 不收到响应。

### Business 连接场景

- 新机器人：新服务 ready 后绑定一次。
- 已绑定的旧机器人迁到全新 VPS/数据库：通常在 ready 后断开并重连一次；如果
  `/health/ready` 已显示 `connection=candidate`，先不要重连。
- 恢复已 claim 且含可信连接的数据库：不要重连。
- claim 返回 `connection=missing` 仍然有效，可以随后建立连接。

Webhook 生命周期 payload 只是触发器；`getBusinessConnection` 才是 enabled、四项
rights、Business 用户和连接世代的当前权威来源。匹配连接仍在协调时，Business
副作用会被阻止；keyless 的私聊/Owner 设置消息仍可投递。

可选诊断：

```bash
set -a
source .env
set +a
curl --fail --silent --show-error \
  "https://api.telegram.org/bot${CHATHYGIENE_BOT_TOKEN}/getWebhookInfo"
```

`getWebhookInfo` 能检查 Telegram 可见的 URL、pending count 和错误，但不会显示、
也不能证明当前 webhook secret。

### 启动与健康状态

每次启动严格按以下单向顺序执行：

1. 数据库描述和 orphan claim 路径预检；
2. migration；
3. 提交/加载主种子；
4. 派生 bot-independent challenge key 与 Owner-claim token；
5. 用 claim token 只读验证现有 claim target/temp；
6. 在事务外执行认证 `getMe`；
7. pin 或验证正数 bot ID；
8. 派生 bot-bound webhook secret；
9. 进入全局 `PENDING` gate；
10. 恢复旧连接事实；
11. 导入/加载不可变 Owner；
12. 对至多一个可信连接做当前状态查询；
13. 恢复本地非连接事件；
14. 把开放的旧 challenge 重新签名到 version 1；
15. 协调 `claim-code`；
16. 绑定内部 `8080`；
17. 自动提交完整 webhook 声明，且 `drop_pending_updates=false`；
18. 启动 notifier、processing、outbox worker；
19. 开始 HTTP serve；
20. drain 剩余已记录连接触发器；
21. 全局状态转为 `READY`。

`GET /health/live` 在 HTTP 已 serve 时总是 `200`，用于 Docker/路由 liveness。
`GET /health/ready` 是状态和告警信号；只有全局 `READY` 且 Owner/连接组合合法时才
返回 `200`，响应仅含：

```json
{"status":"ok","owner":"claimed","connection":"enabled"}
```

`owner` 为 `claimed|unclaimed`；`connection` 为
`missing|candidate|enabled|disabled|rights_incomplete|ambiguous`。所有
`PENDING`、`AUTH_FAILED`、可信连接仍 `PENDING` 或损坏/矛盾组合都返回同一个最小
`503`：

```json
{"status":"unavailable"}
```

Owner 的 Telegram `/health` 命令使用同一事务分类，并额外显示 dry-run。它与两个
HTTP probe 不是同一个接口。

反向代理或 orchestrator 不得因为 readiness 为 `503` 或 Docker 暂时 unhealthy 就
停止转发 `/telegram/webhook`。进程 live 时必须保留 ingress，`PENDING` 协调可能正
需要新的连接触发器才能完成。10 分钟 container start period 覆盖有界的 pre-serve
控制面调用：`getMe` 最坏 25 秒、至多一次可信 `getBusinessConnection` 最坏 25 秒、
webhook 协调最坏 430 秒，共 480 秒；另外 120 秒留给 migration、seed/pin、challenge
重签名、claim 文件、listener/worker 和调度。它不承诺吸收任意历史 backlog。

修改 `CHATHYGIENE_PUBLIC_WEBHOOK_URL` 或 bot token 后必须重启；下一次启动会先自动
协调 Telegram，再开放 readiness。

### Bot 身份与失败边界

第一次认证成功的 `getMe` 会把正数 bot ID 存入数据库。这是 trust-on-first-use：有了
pin 之后，同一 bot 的轮换 token 可被机械接受，另一个 bot 会被拒绝。旧数据库在首
次升级前没有 bot ID，因此无法判断运维人员是否误填了另一个 bot 的有效 token。
第一次升级必须使用旧 bot 的当前 token，记录返回的 bot ID 与凭据来源/证明，但绝不
记录 token；不要把首次升级和更换 bot 合并。

启动 `getMe` 的 `401/403` 会在 bot pin、全局状态和可信连接发生变化前返回脱敏错误，
HTTP 不会启动。启动期可信连接查询的 `401/403` 会提交全局 `AUTH_FAILED` 并终止
启动；serve 后恢复遇到相同错误会先让 readiness 变为最小 `503`，再通过受控 worker
清理退出。只有明确识别的 connection-not-found 是连接级结果；timeout、协议错误或
未知拒绝都会保留触发器等待重试，而不会信任旧 payload。

### 数据库、备份和权限

数据库现在包含主种子、Owner、Business 状态、挑战与运行时设置。只有 `.env` 不能
恢复这些状态；只有 SQLite 备份也不能运行服务。完整恢复需要停机后的
`chathygiene.db`、可能存在的 `-wal`/`-shm`，以及 bot token、公开 URL、部署/TLS 等
不可派生配置。

权限要求：

- `data/`：部署 owner，`0700`；
- DB、WAL、SHM、`claim-code` 与数据库备份：`0600`；
- 活跃 `.env` 和回滚 `.env` 备份：部署 owner，`0600`；
- 回滚环境备份必须在 Git checkout 和 Docker build context 之外，例如
  `/var/backups/chathygiene/pre-upgrade.env`。

容器以 `umask 077` 创建新文件，并对默认 `/data` 与现有 DB/sidecar 做 fail-closed
检查。镜像默认以 root 运行时，bind mount 中的新文件可能是 root-owned；安全读取
claim code 可使用容器命令，不能承诺每个宿主用户都可直接读取。数据库主种子能派生
claim code，所以只保护同级文件是不够的。

恢复 `CLAIMED` 数据库会恢复同一个 Owner，不再 claim，并只删除精确安全的陈旧
claim 文件。恢复 `UNCLAIMED`/claim 前备份会重建同一 code，并可提升保留下来的匹配
候选连接。绝不能让任何两个数据库同时使用同一个 Telegram bot；它们会争夺单一
webhook，克隆数据库还共享派生凭据。

### 一次性升级

1. 在旧实例执行 `/dry_run on` 并确认 `/health`。
2. 停止旧实例；不要 chmod 或复制仍在运行的 SQLite/WAL。
3. 备份三个精确数据库路径中实际存在的文件，并把旧 `code/.env` 复制到 checkout/
   build context 之外的受保护路径：

   ```bash
   docker compose stop chathygiene
   sudo install -d -m 0700 /var/backups/chathygiene
   sudo install -m 0600 .env /var/backups/chathygiene/pre-upgrade.env
   cp -p data/chathygiene.db /var/backups/chathygiene/
   test ! -e data/chathygiene.db-wal || cp -p data/chathygiene.db-wal /var/backups/chathygiene/
   test ! -e data/chathygiene.db-shm || cp -p data/chathygiene.db-shm /var/backups/chathygiene/
   checkout_path="$(realpath .)"
   rollback_path="$(realpath /var/backups/chathygiene/pre-upgrade.env)"
   case "$rollback_path" in "$checkout_path"|"$checkout_path"/*) exit 1 ;; esac
   ```

4. 用编辑器从活跃 `.env` 删除 `CHATHYGIENE_WEBHOOK_SECRET`、
   `CHATHYGIENE_CHALLENGE_HMAC_KEY`、`CHATHYGIENE_OWNER_USER_ID`，加入公开 URL；旧值
   只留在外部回滚备份中。不要打印环境值。
5. 在服务停止时，对每个实际存在的 DB/WAL/SHM、数据库备份、活跃 `.env`、外部回滚
   `.env` 分别执行精确 `chmod 600`，并对 `data` 执行 `chmod 700`。需要时用 `sudo
   chown deployment:deployment <精确路径>` 修正 owner；不要使用递归 glob。

   ```bash
   chmod 700 data
   chmod 600 .env data/chathygiene.db
   test ! -e data/chathygiene.db-wal || chmod 600 data/chathygiene.db-wal
   test ! -e data/chathygiene.db-shm || chmod 600 data/chathygiene.db-shm
   sudo chmod 600 /var/backups/chathygiene/pre-upgrade.env
   sudo chmod 600 /var/backups/chathygiene/chathygiene.db
   ```
6. 使用同一 bot 的当前 token 启动新镜像，不要在首次 pin 时换 bot。
7. 检查 `/health/live`、`/health/ready`、Owner `/health`、可选 `getWebhookInfo` 和
   dry-run。旧数据库没有可信连接时才 claim。

升级恢复已记录事件后，恰好一个旧 Business 连接会被导入为 Owner；零个则变为
`UNCLAIMED`，多个会 fail closed。开放 challenge 会自动重签名，正确答案仍有效。新
进程自然忽略三个旧变量，但活跃 Compose 环境仍应移除它们；验收只检查变量名是否
存在，绝不输出值。

### 回滚

回滚必须同时恢复旧镜像、升级前完整数据库集和外部保存的旧 `.env`：

```bash
docker compose stop chathygiene
install -m 0600 /var/backups/chathygiene/pre-upgrade.env .env
# 恢复升级前 chathygiene.db、-wal、-shm 与旧镜像后再启动
set -a
source .env
set +a
curl --fail --silent --show-error --request POST \
  "https://api.telegram.org/bot${CHATHYGIENE_BOT_TOKEN}/setWebhook" \
  --data-urlencode "url=https://host/telegram/webhook" \
  --data-urlencode "secret_token=${CHATHYGIENE_WEBHOOK_SECRET}" \
  --data-urlencode "drop_pending_updates=false"
```

上面的手动 webhook 命令只用于兼容旧版本回滚。恢复备份会丢弃升级后状态。编辑
`_sqlx_migrations` 或删除 `key_material` 不是回滚或轮换方法。

Version 1 不支持原地轮换主种子，也没有 Owner reset/rebind 命令。删除或修改任一
singleton 都是损坏，不是轮换。未来 rebind 设计必须让 Telegram 历史或 claim 前备份
中保留的旧 token 永远不能重新生效。

## 使用

Owner 命令必须从普通机器人私聊发送：

| 命令 | 作用 |
| --- | --- |
| `/start` | claim 前指向 `claim-code`；claim 后向 Owner 显示帮助。 |
| `/health` | 显示共享 Owner/连接分类与 `dry_run=on|off`。 |
| `/dry_run on` | 随时增加安全性并持久化。 |
| `/dry_run off` | 仅在 enabled、四项 rights 完整的可信连接下启用真实操作。 |
| `/errors [1..20]` | 查看脱敏错误。 |
| `/inspect <chat_id>` | 查看已知对话状态。 |
| `/reset <chat_id>` | 删除机器人已知消息并重置生命周期。 |
| `/unblock <chat_id>` | 清除本地软屏蔽。 |
| `/mark_spam` / `/mark_ham` | 标注回复的文本/说明样本。 |

先在 dry-run 中用非联系人账号验证普通消息与高置信度垃圾消息。只有当 readiness、
Owner `/health`、四项 Business rights、追踪和 `/errors` 都正常时才执行
`/dry_run off`。

## 配置

| 变量 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `CHATHYGIENE_BOT_TOKEN` | 是 | — | BotFather token；始终脱敏。 |
| `CHATHYGIENE_PUBLIC_WEBHOOK_URL` | 是 | — | HTTPS 公开 URL；端口限 `443/80/88/8443`。 |
| `CHATHYGIENE_DATABASE_URL` | 否 | `sqlite://data/chathygiene.db` | Compose 固定为 `/data/chathygiene.db`。 |
| `CHATHYGIENE_DESTRUCTIVE_MODE` | 否 | `false` | 仅在没有持久化 runtime setting 时作为启动默认。 |
| `CHATHYGIENE_PORT` | 仅 Compose | `8080` | 宿主映射；应用/容器目标始终是 `8080`。 |
| `RUST_LOG` | 否 | `info` | 日志过滤器；敏感值仍不会输出。 |

应用不读取旧的手动 secret 或数字 Owner 配置。普通消息正文、说明、文件名、用户名、
显示名、bot token、主种子、派生 key、claim token、完整 claim 命令和 HTTP 请求体不
进入应用日志。显式 `/mark_spam`/`/mark_ham` 样本是例外。

## 开发与测试

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets --all-features
docker build -t chathygiene:local .
```

仓库包含 Rust 源码、SQLx migration、Docker/Compose 配置和集成测试。服务继续使用
单连接 SQLite pool、一个 processing worker、容量 `32/128` 和 `250ms` outbox poll。

## 故障排查

| 症状 | 原因/处理 |
| --- | --- |
| 缺失/损坏 `key_material`、未知 key version 或 checksum 不匹配 | fail closed；恢复完整备份，不要删除 singleton。 |
| Pinned bot mismatch | 使用该数据库原 bot 的 token；不要修改 pin。 |
| 启动 `getMe` 认证失败 | 脱敏退出且不 serve；修复 token 后重启。 |
| `AUTH_FAILED` 或可信/trigger 协调长期 pending | 修复 bot 权限/网络并保留 ingress；触发器会重试。 |
| Owner singleton 无效或旧可信连接多于一个 | fail closed；从一致备份恢复，禁止手工挑选。 |
| 缺失数据库旁有 orphan claim 文件，或 claim 文件属于另一个数据库 | 停机核对数据库与文件来源；不要让应用覆盖。 |
| claim target/temp 是 symlink、错误 owner、错误模式或内容不匹配 | 停机后精确修复/移走；应用不会自动删除。 |
| `data`、DB sidecar、活跃/回滚 `.env` 权限过宽 | 停机后按精确路径修正为 `0700/0600`。 |
| 旧 challenge 表达式畸形或 HMAC version 不支持 | 启动失败；恢复未损坏备份。 |
| 公开端口不支持或 URL 不是 HTTPS | 改用 `443/80/88/8443` 和 TLS 终止代理。 |
| proxy 返回 `404/403` | 核对公开路径重写与 secret header 保留。 |
| 自动 webhook 永久拒绝或瞬态重试耗尽 | 查看脱敏类别/状态码，修复 URL、bot 或网络后重启。 |
| candidate 缺失/过期 | 重新连接一次以产生新的触发器。 |
| `connection=ambiguous` | 不会自动选一条；等待旧候选过期后做一次新的同 Owner 连接。 |
| readiness 为 `503` 但 live 为 `200` | 保持 webhook 转发；根据 Owner/connection 状态修复。 |
| `/dry_run off` 被拒绝 | 先恢复 enabled 且四项 rights 完整的唯一可信连接。 |

## 许可证

本项目采用 [MIT License](LICENSE)。
