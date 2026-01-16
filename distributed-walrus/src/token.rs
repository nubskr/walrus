use anyhow::{anyhow, Result};
use chrono::{Duration, Utc};
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};

/// JWT secret key - in production, this should be loaded from config or environment
const JWT_SECRET: &[u8] = b"walrus_jwt_secret_key_change_in_production";

/// Token expiration duration (24 hours)
const TOKEN_EXPIRATION_HOURS: i64 = 24;

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    /// Subject (username)
    pub sub: String,
    /// Expiration time (as UTC timestamp)
    pub exp: i64,
    /// Issued at (as UTC timestamp)
    pub iat: i64,
}

impl Claims {
    /// Create new claims for a user
    pub fn new(username: String) -> Self {
        let now = Utc::now();
        let exp = now + Duration::hours(TOKEN_EXPIRATION_HOURS);

        Self {
            sub: username,
            exp: exp.timestamp(),
            iat: now.timestamp(),
        }
    }
}

/// Generate a JWT token for a user
pub fn generate_token(username: &str) -> Result<String> {
    let claims = Claims::new(username.to_string());
    let token = encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(JWT_SECRET),
    )?;
    Ok(token)
}

/// Validate a JWT token and return the username if valid
pub fn validate_token(token: &str) -> Result<String> {
    let validation = Validation::new(Algorithm::HS256);
    let token_data = decode::<Claims>(
        token,
        &DecodingKey::from_secret(JWT_SECRET),
        &validation,
    )
    .map_err(|e| anyhow!("Invalid token: {}", e))?;

    // Check if token is expired (jsonwebtoken already does this, but we can add custom logic)
    let now = Utc::now().timestamp();
    if token_data.claims.exp < now {
        return Err(anyhow!("Token expired"));
    }

    Ok(token_data.claims.sub)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_generation_and_validation() {
        let username = "testuser";
        let token = generate_token(username).unwrap();

        let validated_username = validate_token(&token).unwrap();
        assert_eq!(validated_username, username);
    }

    #[test]
    fn test_invalid_token() {
        let result = validate_token("invalid_token");
        assert!(result.is_err());
    }

    #[test]
    fn test_tampered_token() {
        let token = generate_token("user1").unwrap();
        let mut tampered = token.clone();
        tampered.push_str("tampered");

        let result = validate_token(&tampered);
        assert!(result.is_err());
    }
}
