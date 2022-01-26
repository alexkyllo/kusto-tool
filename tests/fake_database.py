class FakeDatabase:
    """Mock database for testing."""

    def __init__(self, server, database):
        self.server = server
        self.database = database

    def query(self, query):
        """Just return the query instead of running it."""
        return query
