"""
Uc Screening Pydantic Schemas
Data models for request/response validation
"""

from pydantic import BaseModel, Field, ConfigDict, validator
from typing import List, Optional, Dict, Any, Union
from datetime import datetime
from enum import Enum


class TemplatesRequest(BaseModel):
    """Request schema for templates"""
    model_config = ConfigDict(from_attributes=True)
    
    # TODO: Define request fields
    example_field: str = Field(..., description="Example required field")
    optional_field: Optional[int] = Field(None, description="Example optional field")


class TemplatesResponse(BaseModel):
    """Response schema for templates"""
    model_config = ConfigDict(from_attributes=True)
    
    success: bool = Field(True, description="Operation success status")
    data: List[Dict[str, Any]] = Field(..., description="Response data")
    total: int = Field(..., description="Total items count")
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class CriteriaRequest(BaseModel):
    """Request schema for criteria"""
    model_config = ConfigDict(from_attributes=True)
    
    # TODO: Define request fields
    example_field: str = Field(..., description="Example required field")
    optional_field: Optional[int] = Field(None, description="Example optional field")


class CriteriaResponse(BaseModel):
    """Response schema for criteria"""
    model_config = ConfigDict(from_attributes=True)
    
    success: bool = Field(True, description="Operation success status")
    data: List[Dict[str, Any]] = Field(..., description="Response data")
    total: int = Field(..., description="Total items count")
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class FiltersRequest(BaseModel):
    """Request schema for filters"""
    model_config = ConfigDict(from_attributes=True)
    
    # TODO: Define request fields
    example_field: str = Field(..., description="Example required field")
    optional_field: Optional[int] = Field(None, description="Example optional field")


class FiltersResponse(BaseModel):
    """Response schema for filters"""
    model_config = ConfigDict(from_attributes=True)
    
    success: bool = Field(True, description="Operation success status")
    data: List[Dict[str, Any]] = Field(..., description="Response data")
    total: int = Field(..., description="Total items count")
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class ScoreRequest(BaseModel):
    """Request schema for score"""
    model_config = ConfigDict(from_attributes=True)
    
    # TODO: Define request fields
    example_field: str = Field(..., description="Example required field")
    optional_field: Optional[int] = Field(None, description="Example optional field")


class ScoreResponse(BaseModel):
    """Response schema for score"""
    model_config = ConfigDict(from_attributes=True)
    
    success: bool = Field(True, description="Operation success status")
    data: List[Dict[str, Any]] = Field(..., description="Response data")
    total: int = Field(..., description="Total items count")
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class ProgressRequest(BaseModel):
    """Request schema for progress"""
    model_config = ConfigDict(from_attributes=True)
    
    # TODO: Define request fields
    example_field: str = Field(..., description="Example required field")
    optional_field: Optional[int] = Field(None, description="Example optional field")


class ProgressResponse(BaseModel):
    """Response schema for progress"""
    model_config = ConfigDict(from_attributes=True)
    
    success: bool = Field(True, description="Operation success status")
    data: List[Dict[str, Any]] = Field(..., description="Response data")
    total: int = Field(..., description="Total items count")
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class ResultsRequest(BaseModel):
    """Request schema for results"""
    model_config = ConfigDict(from_attributes=True)
    
    # TODO: Define request fields
    example_field: str = Field(..., description="Example required field")
    optional_field: Optional[int] = Field(None, description="Example optional field")


class ResultsResponse(BaseModel):
    """Response schema for results"""
    model_config = ConfigDict(from_attributes=True)
    
    success: bool = Field(True, description="Operation success status")
    data: List[Dict[str, Any]] = Field(..., description="Response data")
    total: int = Field(..., description="Total items count")
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class HeatmapRequest(BaseModel):
    """Request schema for heatmap"""
    model_config = ConfigDict(from_attributes=True)
    
    # TODO: Define request fields
    example_field: str = Field(..., description="Example required field")
    optional_field: Optional[int] = Field(None, description="Example optional field")


class HeatmapResponse(BaseModel):
    """Response schema for heatmap"""
    model_config = ConfigDict(from_attributes=True)
    
    success: bool = Field(True, description="Operation success status")
    data: List[Dict[str, Any]] = Field(..., description="Response data")
    total: int = Field(..., description="Total items count")
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class TopLocationsRequest(BaseModel):
    """Request schema for top_locations"""
    model_config = ConfigDict(from_attributes=True)
    
    # TODO: Define request fields
    example_field: str = Field(..., description="Example required field")
    optional_field: Optional[int] = Field(None, description="Example optional field")


class TopLocationsResponse(BaseModel):
    """Response schema for top_locations"""
    model_config = ConfigDict(from_attributes=True)
    
    success: bool = Field(True, description="Operation success status")
    data: List[Dict[str, Any]] = Field(..., description="Response data")
    total: int = Field(..., description="Total items count")
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class FilterRequest(BaseModel):
    """Request schema for filter"""
    model_config = ConfigDict(from_attributes=True)
    
    # TODO: Define request fields
    example_field: str = Field(..., description="Example required field")
    optional_field: Optional[int] = Field(None, description="Example optional field")


class FilterResponse(BaseModel):
    """Response schema for filter"""
    model_config = ConfigDict(from_attributes=True)
    
    success: bool = Field(True, description="Operation success status")
    data: List[Dict[str, Any]] = Field(..., description="Response data")
    total: int = Field(..., description="Total items count")
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class ShortlistRequest(BaseModel):
    """Request schema for shortlist"""
    model_config = ConfigDict(from_attributes=True)
    
    # TODO: Define request fields
    example_field: str = Field(..., description="Example required field")
    optional_field: Optional[int] = Field(None, description="Example optional field")


class ShortlistResponse(BaseModel):
    """Response schema for shortlist"""
    model_config = ConfigDict(from_attributes=True)
    
    success: bool = Field(True, description="Operation success status")
    data: List[Dict[str, Any]] = Field(..., description="Response data")
    total: int = Field(..., description="Total items count")
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class AddToProjectRequest(BaseModel):
    """Request schema for add_to_project"""
    model_config = ConfigDict(from_attributes=True)
    
    # TODO: Define request fields
    example_field: str = Field(..., description="Example required field")
    optional_field: Optional[int] = Field(None, description="Example optional field")


class AddToProjectResponse(BaseModel):
    """Response schema for add_to_project"""
    model_config = ConfigDict(from_attributes=True)
    
    success: bool = Field(True, description="Operation success status")
    data: List[Dict[str, Any]] = Field(..., description="Response data")
    total: int = Field(..., description="Total items count")
    timestamp: datetime = Field(default_factory=datetime.utcnow)

