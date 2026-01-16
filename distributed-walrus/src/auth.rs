use anyhow::{anyhow, Result};
use bcrypt::{hash, verify, DEFAULT_COST};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

/// User credentials stored in the cluster state
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct User {
    pub username: String,
    pub password_hash: String,
    pub role: UserRole,
    /// Permanent API key for authentication (never expires)
    pub api_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum UserRole {
    /// Admin can manage users and access all operations
    Admin,
    /// Producer can only publish messages (PUT, REGISTER)
    Producer,
    /// Consumer can only read messages (GET, STATE, METRICS)
    Consumer,
}

impl User {
    /// Create a new user with hashed password and generate permanent API key
    pub fn new(username: String, password: &str, role: UserRole) -> Result<Self> {
        let password_hash = hash(password, DEFAULT_COST)?;
        let api_key = Self::generate_api_key(&username, &role);
        Ok(User {
            username,
            password_hash,
            role,
            api_key,
        })
    }

    /// Generate a permanent API key based on username and role
    /// Format: walrus_<role>_<hash>
    fn generate_api_key(username: &str, role: &UserRole) -> String {
        let role_prefix = match role {
            UserRole::Admin => "admin",
            UserRole::Producer => "prod",
            UserRole::Consumer => "cons",
        };

        // Generate a unique hash based on username, role, and timestamp
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        let input = format!("{}:{}:{}", username, role_prefix, timestamp);
        let mut hasher = Sha256::new();
        hasher.update(input.as_bytes());
        let hash = hasher.finalize();
        let hash_hex = format!("{:x}", hash);

        // Take first 32 characters of hash for readability
        format!("walrus_{}_{}",  role_prefix, &hash_hex[..32])
    }

    /// Verify password against stored hash
    pub fn verify_password(&self, password: &str) -> Result<bool> {
        Ok(verify(password, &self.password_hash)?)
    }

    /// Check if user can perform write operations (PUT, REGISTER)
    pub fn can_write(&self) -> bool {
        matches!(self.role, UserRole::Admin | UserRole::Producer)
    }

    /// Check if user can perform read operations (GET, STATE, METRICS)
    pub fn can_read(&self) -> bool {
        matches!(self.role, UserRole::Admin | UserRole::Consumer)
    }

    /// Check if user is admin
    pub fn is_admin(&self) -> bool {
        matches!(self.role, UserRole::Admin)
    }
}

/// Authentication manager for handling user operations
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AuthManager {
    users: HashMap<String, User>,
    /// Index for fast API key lookup: api_key -> username
    #[serde(skip)]
    api_key_index: HashMap<String, String>,
}

impl AuthManager {
    pub fn new() -> Self {
        Self {
            users: HashMap::new(),
            api_key_index: HashMap::new(),
        }
    }

    /// Rebuild API key index (called after deserialization)
    fn rebuild_api_key_index(&mut self) {
        self.api_key_index.clear();
        for (username, user) in &self.users {
            self.api_key_index.insert(user.api_key.clone(), username.clone());
        }
    }

    /// Add a new user
    pub fn add_user(&mut self, user: User) -> Result<()> {
        if self.users.contains_key(&user.username) {
            return Err(anyhow!("User already exists"));
        }
        self.api_key_index.insert(user.api_key.clone(), user.username.clone());
        self.users.insert(user.username.clone(), user);
        Ok(())
    }

    /// Remove a user
    pub fn remove_user(&mut self, username: &str) -> Result<()> {
        let user = self.users
            .remove(username)
            .ok_or_else(|| anyhow!("User not found"))?;
        self.api_key_index.remove(&user.api_key);
        Ok(())
    }

    /// Authenticate a user with username and password (returns API key)
    pub fn authenticate(&self, username: &str, password: &str) -> Result<&User> {
        let user = self
            .users
            .get(username)
            .ok_or_else(|| anyhow!("Invalid username or password"))?;

        if !user.verify_password(password)? {
            return Err(anyhow!("Invalid username or password"));
        }

        Ok(user)
    }

    /// Authenticate a user with API key (fast, no password verification)
    pub fn authenticate_with_api_key(&mut self, api_key: &str) -> Option<&User> {
        // Rebuild index if empty (after deserialization)
        if self.api_key_index.is_empty() && !self.users.is_empty() {
            self.rebuild_api_key_index();
        }

        let username = self.api_key_index.get(api_key)?;
        self.users.get(username)
    }

    /// Check if user exists
    pub fn user_exists(&self, username: &str) -> bool {
        self.users.contains_key(username)
    }

    /// Get user by username
    pub fn get_user(&self, username: &str) -> Option<&User> {
        self.users.get(username)
    }

    /// List all users
    pub fn list_users(&self) -> Vec<&User> {
        self.users.values().collect()
    }

    /// Check if there are any users
    pub fn is_empty(&self) -> bool {
        self.users.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_user_creation_and_verification() {
        let user = User::new("test_user".to_string(), "password123", UserRole::Producer).unwrap();
        assert!(user.verify_password("password123").unwrap());
        assert!(!user.verify_password("wrong_password").unwrap());
        assert!(user.api_key.starts_with("walrus_prod_"));
        assert!(user.can_write());
        assert!(!user.can_read());
    }

    #[test]
    fn test_user_permissions() {
        let admin = User::new("admin".to_string(), "pass", UserRole::Admin).unwrap();
        assert!(admin.can_write());
        assert!(admin.can_read());
        assert!(admin.is_admin());

        let producer = User::new("prod".to_string(), "pass", UserRole::Producer).unwrap();
        assert!(producer.can_write());
        assert!(!producer.can_read());
        assert!(!producer.is_admin());

        let consumer = User::new("cons".to_string(), "pass", UserRole::Consumer).unwrap();
        assert!(!consumer.can_write());
        assert!(consumer.can_read());
        assert!(!consumer.is_admin());
    }

    #[test]
    fn test_auth_manager() {
        let mut manager = AuthManager::new();
        let user = User::new("alice".to_string(), "secret", UserRole::Admin).unwrap();
        let api_key = user.api_key.clone();

        manager.add_user(user.clone()).unwrap();
        assert!(manager.user_exists("alice"));

        let authenticated = manager.authenticate("alice", "secret").unwrap();
        assert_eq!(authenticated.username, "alice");

        assert!(manager.authenticate("alice", "wrong").is_err());
        assert!(manager.authenticate("bob", "secret").is_err());

        // Test API key authentication
        let auth_by_key = manager.authenticate_with_api_key(&api_key).unwrap();
        assert_eq!(auth_by_key.username, "alice");
    }
}
