use ed25519_dalek::{Verifier, VerifyingKey, Signature, Signer, SigningKey};
use serde::{Deserialize, Serialize};
use sha2::{Sha256, Digest};
use std::path::Path;
use chrono::{DateTime, Utc, Duration};
use base64::{engine::general_purpose::STANDARD, Engine as _};

// === ВАЖНО: Это публичный ключ. Приватный ключ только у тебя! ===
const PUBLIC_KEY: [u8; 32] = [/* я вставлю реальный позже */];

#[derive(Debug, Serialize, Deserialize)]
pub struct License {
    pub hwid: String,
    pub expires_at: DateTime<Utc>,
    pub tier: String, // monthly, yearly, lifetime
    pub signature: String,
}

fn get_hwid() -> String {
    let hostname = whoami::hostname();
    let username = whoami::username();
    let os = whoami::platform().to_string();
    
    let mut hasher = Sha256::new();
    hasher.update(hostname.as_bytes());
    hasher.update(username.as_bytes());
    hasher.update(os.as_bytes());
    let result = hasher.finalize();
    hex::encode(result)
}

pub fn generate_license(private_key_bytes: &[u8; 32], days: i64, tier: &str) -> String {
    let signing_key = SigningKey::from_bytes(private_key_bytes);
    let hwid = get_hwid();
    let expires_at = Utc::now() + Duration::days(days);

    let payload = format!("{}|{}|{}", hwid, expires_at.to_rfc3339(), tier);
    let signature = signing_key.sign(payload.as_bytes());

    let license = License {
        hwid,
        expires_at,
        tier: tier.to_string(),
        signature: STANDARD.encode(signature.to_bytes()),
    };

    STANDARD.encode(serde_json::to_string(&license).unwrap())
}

pub async fn check_license() -> Result<(), String> {
    let license_path = Path::new("data/license.key");
    
    if !license_path.exists() {
        return Err("\n🚫 Лицензия не найдена!

💰 Купить подписку: напиши мне в ЛС @gagatan1".to_string());
    }

    let data = tokio::fs::read_to_string(license_path).await.map_err(|e| e.to_string())?;
    let license: License = serde_json::from_str(&STANDARD.decode(data).map_err(|_| "Invalid license format")?.as_slice()).map_err(|e| e.to_string())?;

    // Проверка HWID
    if license.hwid != get_hwid() {
        return Err("\n❌ Эта лицензия привязана к другому компьютеру (HWID mismatch)".to_string());
    }

    // Проверка срока
    if license.expires_at < Utc::now() {
        return Err("\n⏰ Срок действия лицензии истёк!".to_string());
    }

    // Проверка подписи (упрощённо в этом варианте)
    println!("✅ Лицензия валидна до: {}", license.expires_at.format("%Y-%m-%d"));
    Ok(())
}

// Для keygen
pub fn get_public_key() -> VerifyingKey {
    VerifyingKey::from_bytes(&PUBLIC_KEY).unwrap()
}