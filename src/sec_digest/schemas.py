"""Pydantic schemas for structured extraction from SEC News Digest."""

from datetime import date
from typing import Optional, List, Literal
from pydantic import BaseModel, Field


class Entity(BaseModel):
    """An entity involved in an enforcement action."""

    name: str = Field(description="Full name of the entity (person or company)")
    entity_type: Optional[Literal["individual", "company", "other"]] = Field(
        default=None,
        description="Type of entity"
    )
    location: Optional[str] = Field(
        default=None,
        description="City, state, or country mentioned for this entity"
    )


class Violation(BaseModel):
    """A securities law violation."""

    statute: Optional[str] = Field(
        default=None,
        description="The statute violated (e.g., 'Section 10(b)', 'Rule 10b-5')"
    )
    description: Optional[str] = Field(
        default=None,
        description="Brief description of the violation"
    )


class Sanction(BaseModel):
    """Sanction or penalty imposed."""

    sanction_type: Optional[Literal[
        "suspension", "revocation", "injunction", "fine",
        "imprisonment", "disgorgement", "cease_and_desist", "other"
    ]] = Field(default=None, description="Type of sanction")

    description: str = Field(description="Description of the sanction imposed")

    duration: Optional[str] = Field(
        default=None,
        description="Duration if applicable (e.g., '6 months', '3 years')"
    )

    amount: Optional[str] = Field(
        default=None,
        description="Monetary amount if applicable (e.g., '$50,000')"
    )


class EnforcementAction(BaseModel):
    """A single enforcement action extracted from SEC News Digest."""

    # Metadata
    action_type: Literal["administrative", "civil", "criminal"] = Field(
        description="Type of enforcement proceeding"
    )

    title: Optional[str] = Field(
        default=None,
        description="Title or heading of the enforcement action"
    )

    # Parties
    respondents: List[Entity] = Field(
        default_factory=list,
        description="Entities being charged/sanctioned"
    )

    # Legal details
    violations: List[Violation] = Field(
        default_factory=list,
        description="Violations alleged or found"
    )

    sanctions: List[Sanction] = Field(
        default_factory=list,
        description="Sanctions or penalties imposed"
    )

    # Case information
    settlement: Optional[bool] = Field(
        default=None,
        description="Whether this was a settlement (Offer of Settlement mentioned)"
    )

    court: Optional[str] = Field(
        default=None,
        description="Court name if civil/criminal proceeding"
    )

    case_number: Optional[str] = Field(
        default=None,
        description="Case number or docket number if available"
    )

    release_number: Optional[str] = Field(
        default=None,
        description="SEC Release number (e.g., 'ReI. 34-21595')"
    )

    # Full text for reference
    full_text: str = Field(
        description="Full text of the enforcement action as it appears in the digest"
    )


class DigestExtraction(BaseModel):
    """Extraction results for a single SEC News Digest document."""

    digest_date: date = Field(description="Date of the digest (from filename)")

    has_enforcement_actions: bool = Field(
        description="Whether this digest contains any enforcement actions"
    )

    actions: List[EnforcementAction] = Field(
        default_factory=list,
        description="List of enforcement actions found in this digest"
    )

    extraction_notes: Optional[str] = Field(
        default=None,
        description="Any notes about extraction quality or issues encountered"
    )
