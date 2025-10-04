# Publishing Walrus to crates.io

This guide covers the steps to publish Walrus to crates.io.

## Pre-publication Checklist

### 1. Update Cargo.toml Metadata

Ensure the following fields are correctly set in `Cargo.toml`:

- [ ] `authors` - Replace with actual author information
- [ ] `homepage` - Update with actual repository URL
- [ ] `repository` - Update with actual repository URL
- [ ] `version` - Ensure version follows semantic versioning
- [ ] `description` - Verify description is accurate and under 300 characters
- [ ] `license` - Confirm license is correct (currently MIT)

### 2. Documentation

- [ ] README.md is comprehensive and up-to-date
- [ ] All public APIs have documentation comments
- [ ] Examples in documentation compile and work
- [ ] LICENSE file exists and matches Cargo.toml license field

### 3. Code Quality

- [ ] All tests pass: `cargo test`
- [ ] No clippy warnings: `cargo clippy`
- [ ] Code is formatted: `cargo fmt`
- [ ] Benchmarks run successfully: `make bench-all`

### 4. Version Management

- [ ] Update version in `Cargo.toml`
- [ ] Update version references in README.md
- [ ] Create git tag for the release
- [ ] Update CHANGELOG.md (if exists)

## Publication Steps

### 1. Login to crates.io

```bash
cargo login
```

You'll need an API token from [crates.io](https://crates.io/me).

### 2. Dry Run

Test the publication process without actually publishing:

```bash
cargo publish --dry-run
```

This will:
- Build the package
- Verify all metadata
- Check that all files are included
- Ensure documentation builds

### 3. Publish

If the dry run succeeds, publish to crates.io:

```bash
cargo publish
```

### 4. Verify Publication

- Check that the crate appears on [crates.io](https://crates.io/crates/walrus-rust)
- Verify documentation builds on [docs.rs](https://docs.rs/walrus-rust)
- Test installation: `cargo install walrus-rust --version 0.1.0`

## Post-publication

### 1. Create GitHub Release

- Create a new release on GitHub
- Tag the release with the version number (e.g., `v0.1.0`)
- Include release notes and changelog

### 2. Update Documentation

- Verify docs.rs builds correctly
- Update any external documentation links
- Consider writing a blog post or announcement

### 3. Community

- Share on relevant Rust forums/communities
- Consider submitting to This Week in Rust
- Update any dependency lists or awesome-rust lists

## Troubleshooting

### Common Issues

1. **Missing files**: Check the `exclude` list in Cargo.toml
2. **Documentation fails**: Ensure all doc examples compile
3. **Large package size**: Add more items to `exclude` in Cargo.toml
4. **License issues**: Ensure LICENSE file matches Cargo.toml license field

### Package Size Optimization

Current exclusions in Cargo.toml:
- `target/*` - Build artifacts
- `wal_files/*` - Test WAL files
- `*.csv` - Benchmark output files
- `figures/*` - Documentation images
- `scripts/__pycache__/*` - Python cache files

### Version Strategy

Follow [Semantic Versioning](https://semver.org/):
- `0.1.0` - Initial release
- `0.1.1` - Bug fixes
- `0.2.0` - New features (backward compatible)
- `1.0.0` - Stable API

## Maintenance

### Regular Updates

- Monitor for security vulnerabilities in dependencies
- Keep dependencies up to date
- Respond to issues and pull requests
- Consider API improvements for future versions

### Yanking Versions

If a version has critical bugs:

```bash
cargo yank --vers 0.1.0
```

To un-yank:

```bash
cargo yank --vers 0.1.0 --undo
```

## Resources

- [The Cargo Book - Publishing](https://doc.rust-lang.org/cargo/reference/publishing.html)
- [crates.io Guide](https://doc.rust-lang.org/cargo/reference/publishing.html)
- [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/)
- [docs.rs documentation](https://docs.rs/about)
