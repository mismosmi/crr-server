use crate::error::CRRError;
use lettre::Transport;

pub(crate) fn send_email(receiver: &str, subject: String, message: String) -> Result<(), CRRError> {
    let credentials = lettre::transport::smtp::authentication::Credentials::new(
        std::env::var("SMTP_USERNAME")?,
        std::env::var("SMTP_PASSWORD")?,
    );
    let mailer = lettre::SmtpTransport::relay(&std::env::var("SMTP_SERVER")?)?
        .credentials(credentials)
        .port(465)
        .build();

    let sender = std::env::var("SMTP_SENDER")?;
    let email = lettre::Message::builder()
        .from(sender.parse().map_err(|_err| {
            CRRError::SmtpError(format!("Failed to parse sender: \"{}\"", sender))
        })?)
        .to(receiver.parse().map_err(|_err| {
            CRRError::SmtpError(format!("Failed to parse receiver: \"{}\"", receiver))
        })?)
        .subject(subject)
        .body(message)?;

    mailer.send(&email)?;

    Ok(())
}
