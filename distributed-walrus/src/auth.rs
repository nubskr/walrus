use anyhow::{anyhow, Result};
use bcrypt::{hash, verify, DEFAULT_COST};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// User credentials stored in the cluster state
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct User {
    pub username: String,
    pub password_hash: String,
    pub role: UserRole,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum UserRole {
    Admin,
    User,
}

impl User {
    /// Create a new user with hashed password
    pub fn new(username: String, password: &str, role: UserRole) -> Result<Self> {
        let password_hash = hash(password, DEFAULT_COST)?;
        Ok(User {
            username,
            password_hash,
            role,
        })
    }

    /// Verify password against stored hash
    pub fn verify_password(&self, password: &str) -> Result<bool> {
        Ok(verify(password, &self.password_hash)?)
    }
}

/// Authentication manager for handling user operations
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AuthManager {
    users: HashMap<String, User>,
}

impl AuthManager {
    pub fn new() -> Self {
        Self {
            users: HashMap::new(),
        }
    }

    /// Add a new user
    pub fn add_user(&mut self, user: User) -> Result<()> {
        if self.users.contains_key(&user.username) {
            return Err(anyhow!("User already exists"));
        }
        self.users.insert(user.username.clone(), user);
        Ok(())
    }

    /// Remove a user
    pub fn remove_user(&mut self, username: &str) -> Result<()> {
        self.users
            .remove(username)
            .ok_or_else(|| anyhow!("User not found"))?;
        Ok(())
    }

    /// Authenticate a user
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
        let user = User::new("test_user".to_string(), "password123", UserRole::User).unwrap();
        assert!(user.verify_password("password123").unwrap());
        assert!(!user.verify_password("wrong_password").unwrap());
    }

    #[test]
    fn test_auth_manager() {
        let mut manager = AuthManager::new();
        let user = User::new("alice".to_string(), "secret", UserRole::Admin).unwrap();

        manager.add_user(user.clone()).unwrap();
        assert!(manager.user_exists("alice"));

        let authenticated = manager.authenticate("alice", "secret").unwrap();
        assert_eq!(authenticated.username, "alice");

        assert!(manager.authenticate("alice", "wrong").is_err());
        assert!(manager.authenticate("bob", "secret").is_err());
    }
}
