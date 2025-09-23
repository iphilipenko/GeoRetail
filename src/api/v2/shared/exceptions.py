"""
Custom exceptions for UC modules
"""


class UCException(Exception):
    """Base exception for UC modules"""
    pass


class ValidationException(UCException):
    """Validation error"""
    pass


class PermissionException(UCException):
    """Permission denied"""
    pass


class DataNotFoundException(UCException):
    """Data not found"""
    pass


class ProcessingException(UCException):
    """Processing error"""
    pass


class ExternalServiceException(UCException):
    """External service error"""
    pass
