# Michel's crr-server

## Setup
Put a .env file in the root dir specifying SMTP credentials like
```bash
SMTP_SERVER=my.smtp.server
SMTP_SENDER="OTP Service <mail@my.mail.provider>"
SMTP_USERNAME=mySMTPusername
SMTP_PASSWORD=mySMTPpassword
```

Then run `cargo run`

## Usage
So far this has 3 endpoints:
```
POST /auth/otp "?email=<email-address>"
```
to receive an otp code per email

```
POST /auth/token "?otp=<otp>"
```
to receive a token via Set-Cookie header.
This endpoint can also be called with a token
set in the cookies to refresh a token.

Default token lifetime is 400 days (which happens to also be the
maximum lifetime for a cookie)

```
POST /database/<databaseName>/migrations/<migrationVersion> "?sql=<migrationCode>"
```
to write a new migration. `<migrationVersion>` starts at 0.


