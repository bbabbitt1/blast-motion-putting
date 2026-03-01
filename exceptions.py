class BlastAPIError(Exception):
    """Base exception for all Blast API errors"""
    pass

class BlastAuthError(BlastAPIError):
    """Raised when authentication fails or session expires (401)"""
    pass

class BlastRateLimitError(BlastAPIError):
    """Raised when rate limited by the server (429)"""
    pass

class BlastServerError(BlastAPIError):
    """Raised when server returns a 5xx error"""
    pass

class BlastParseError(BlastAPIError):
    """Raised when API response cannot be parsed"""
    pass

class BlastDataError(BlastAPIError):
    """Raised when data is missing or malformed"""
    pass