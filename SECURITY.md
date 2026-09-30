# Security

NewsWave is paper-trading software, but it handles brokerage API keys.

- Report a vulnerability privately through GitHub's "Report a vulnerability" button on the Security tab of
  https://github.com/AskElira/newswave, not in a public issue.
- Never put API keys, secrets, `.env` contents or unredacted logs in an issue, pull request or discussion.
  If you pasted a key anywhere public, revoke it at Alpaca immediately.
- The paper-only lock, the arm gate and the classifier isolation are safety features. A way to bypass any of
  them is a security issue.
