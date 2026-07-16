# DialogHub

Telegram CRM that collects incoming conversations from authorised work accounts in one forum group.

## How it works

1. Add the bot as an administrator to a Telegram supergroup with Topics enabled.
2. An administrator sends `/setup` in that group.
3. Add Pyrogram `.session` files to `ACCOUNT_SESSIONS_DIR` and assign them to projects with `/project` and `/account`.
4. Each incoming private message creates (or reuses) a topic. Replies in the topic are delivered through the matching work account.

Secrets live only in `.env`, never in this repository.
