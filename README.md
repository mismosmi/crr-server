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
So far this has 4 endpoints:
```
POST /auth/otp "?email=<email-address>"
```
to receive an otp code per email

```
POST /auth/refresh_token "?otp=<otp>"
```
to receive a refresh token via Set-Cookie header

```
POST /auth/access_token
```
(obviously with the refresh token cookie) to receive an access token via Set-Cookie header

```
POST /database/<databaseName>/migrations/<migrationVersion> "?sql=<migrationCode>"
```
to write a new migration. `<migrationVersion>` starts at 0.


