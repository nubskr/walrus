def pytest_configure(config):
    config.addinivalue_line("markers", "soak_long: extended-duration soak test (20m default)")
