# Walrus Authentication System

Complete guide for using the API key-based authentication system with role-based access control.

## Overview

Walrus uses **permanent API keys** for authentication with three role types:
- **Admin**: Full access (manage users + read/write operations)
- **Producer**: Write-only access (PUT, REGISTER operations)
- **Consumer**: Read-only access (GET, STATE, METRICS operations)

## Quick Start

### 1. Configure Passwords (Production)

```bash
# Copy example configuration
cp .env.example .env

# Edit with strong passwords
nano .env

# Set environment variables
export WALRUS_ADMIN_PASSWORD="your_secure_password_here"
export WALRUS_PRODUCER_PASSWORD="another_secure_password"
export WALRUS_CONSUMER_PASSWORD="yet_another_password"

# Start server
cargo run
```

### 2. First Login

```bash
# Connect to server
telnet localhost 8080

# Login with admin credentials
LOGIN admin your_secure_password_here

# Server returns:
OK authenticated
API_KEY: walrus_admin_a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6
ROLE: Admin

Save this API key and use 'APIKEY <key>' for future connections
```

### 3. Use API Key for Future Connections

```bash
# Reconnect and authenticate with API key (fast!)
telnet localhost 8080
APIKEY walrus_admin_a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6

# Server returns:
OK authenticated as admin (role: Admin)

# Now you can use all commands
REGISTER my_topic
PUT my_topic hello_world
GET my_topic
```

## Environment Variables

Configure passwords via environment variables:

| Variable | Description | Default (Dev Only) |
|----------|-------------|-------------------|
| `WALRUS_ADMIN_PASSWORD` | Admin user password | `admin123` |
| `WALRUS_PRODUCER_PASSWORD` | Producer user password | `producer123` |
| `WALRUS_CONSUMER_PASSWORD` | Consumer user password | `consumer123` |

**⚠️ WARNING**: Default passwords are for development only. Always set custom passwords for production!

## Default Users

When first started, the system creates these default users:

| Username | Password (Default) | Role | Capabilities |
|----------|-------------------|------|--------------|
| admin | admin123 | Admin | Full access: user management + all operations |
| producer1 | producer123 | Producer | Write only: PUT, REGISTER |
| consumer1 | consumer123 | Consumer | Read only: GET, STATE, METRICS |

## Authentication Commands

### LOGIN (username/password → API key)
```
LOGIN <username> <password>
```
Returns permanent API key. Use this command once, then save the API key.

**Example:**
```
LOGIN admin admin123
→ OK authenticated
  API_KEY: walrus_admin_...
  ROLE: Admin
```

### APIKEY (fast authentication)
```
APIKEY <your_api_key>
```
Authenticate using permanent API key. Much faster than LOGIN (no password hashing).

**Example:**
```
APIKEY walrus_admin_a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6
→ OK authenticated as admin (role: Admin)
```

## Admin Commands

Only Admin users can create new users:

### Create Producer
```
ADDPRODUCER <username> <password>
```

**Example:**
```
ADDPRODUCER myproducer SecurePass123!
→ OK Producer added
  API_KEY: walrus_prod_...
```

### Create Consumer
```
ADDCONSUMER <username> <password>
```

**Example:**
```
ADDCONSUMER myconsumer SecurePass456!
→ OK Consumer added
  API_KEY: walrus_cons_...
```

## Role Permissions

| Operation | Admin | Producer | Consumer |
|-----------|-------|----------|----------|
| REGISTER (create topic) | ✓ | ✓ | ✗ |
| PUT (publish message) | ✓ | ✓ | ✗ |
| GET (read message) | ✓ | ✗ | ✓ |
| STATE (topic state) | ✓ | ✗ | ✓ |
| METRICS (cluster metrics) | ✓ | ✗ | ✓ |
| ADDPRODUCER (create producer) | ✓ | ✗ | ✗ |
| ADDCONSUMER (create consumer) | ✓ | ✗ | ✗ |

## Security Best Practices

### 1. Password Management
```bash
# ✓ Generate strong passwords
pwgen -s 32 1

# ✓ Use different passwords for each role
export WALRUS_ADMIN_PASSWORD="$(pwgen -s 32 1)"
export WALRUS_PRODUCER_PASSWORD="$(pwgen -s 32 1)"
export WALRUS_CONSUMER_PASSWORD="$(pwgen -s 32 1)"

# ✓ Never commit passwords to git
# ✗ Don't use default passwords in production
# ✗ Don't share passwords in logs or chat
```

### 2. API Key Management
```bash
# ✓ Store API keys securely (environment variables, secret management)
export MY_PRODUCER_KEY="walrus_prod_..."

# ✓ Use API keys for service-to-service communication
# ✓ One API key per service/application
# ✗ Don't commit API keys to git
# ✗ Don't share API keys between services
```

### 3. Principle of Least Privilege
```bash
# ✓ Use Producer role for services that only publish
ADDPRODUCER metrics_collector SecurePass1!

# ✓ Use Consumer role for services that only read
ADDCONSUMER analytics_dashboard SecurePass2!

# ✗ Don't use Admin role for regular services
```

### 4. Monitoring & Auditing
```bash
# Server logs all authentication attempts
# Monitor logs for:
# - Failed login attempts
# - Unauthorized access attempts
# - Unusual patterns

# Example log entries:
# "User admin authenticated successfully via LOGIN"
# "User producer1 authenticated via API key (role: Producer)"
# "permission denied - this operation requires Admin role"
```

## Production Deployment Checklist

- [ ] Set `WALRUS_ADMIN_PASSWORD` environment variable
- [ ] Set `WALRUS_PRODUCER_PASSWORD` environment variable
- [ ] Set `WALRUS_CONSUMER_PASSWORD` environment variable
- [ ] Verify `.env` file is not committed to git
- [ ] Test login with custom passwords
- [ ] Save API keys securely
- [ ] Create production users with strong passwords
- [ ] Delete or disable default users if not needed
- [ ] Set up log monitoring for security events
- [ ] Document password rotation procedure
- [ ] Set up API key rotation schedule

## Troubleshooting

### "authentication required" error
**Cause**: Trying to execute commands without authentication.

**Solution**:
```
APIKEY <your_api_key>
# or
LOGIN <username> <password>
```

### "invalid API key" error
**Cause**: API key is incorrect or doesn't exist.

**Solution**: Use LOGIN to get a new API key.

### "permission denied" error
**Cause**: User role doesn't have permission for the operation.

**Solution**: Use correct role (Producer for write, Consumer for read, Admin for user management).

### "User already exists" error
**Cause**: Trying to create a user that already exists.

**Solution**: Use a different username or remove existing user first (Admin only).

## API Key Format

API keys follow this format:
```
walrus_<role>_<32_character_hash>
```

Examples:
- Admin: `walrus_admin_a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6`
- Producer: `walrus_prod_f9e8d7c6b5a4a3b2c1d0e1f2g3h4i5j6`
- Consumer: `walrus_cons_1a2b3c4d5e6f7g8h9i0j1k2l3m4n5o6`

API keys:
- Never expire
- Are unique per user
- Cannot be recovered if lost (must login again)
- Are stored in distributed Raft state
- Are validated in < 1ms

## Development vs Production

### Development (Default)
```bash
# No environment variables needed
cargo run

# Use default credentials
LOGIN admin admin123
LOGIN producer1 producer123
LOGIN consumer1 consumer123
```

### Production (Recommended)
```bash
# Set environment variables
export WALRUS_ADMIN_PASSWORD="$(pwgen -s 32 1)"
export WALRUS_PRODUCER_PASSWORD="$(pwgen -s 32 1)"
export WALRUS_CONSUMER_PASSWORD="$(pwgen -s 32 1)"

# Start server
cargo run

# Login with custom password
LOGIN admin <your_password_from_env>

# Create production users
ADDPRODUCER prod_service1 SecurePass1!
ADDCONSUMER cons_service1 SecurePass2!
```

## Support

For issues or questions:
1. Check server logs for error messages
2. Verify environment variables are set correctly
3. Ensure you're using the correct API key for your role
4. Contact system administrator for password resets
