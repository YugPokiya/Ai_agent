"""Ticker universe and business taxonomy for Stage 1."""

from dataclasses import dataclass, asdict
from typing import List


@dataclass(frozen=True)
class CompanyProfile:
    ticker: str
    name: str
    sector_family: str
    sub_vertical: str


TARGET_COMPANIES: List[CompanyProfile] = [
    # Large-cap anchor companies
    CompanyProfile("MSFT", "Microsoft Corporation", "tech", "iaas"),
    CompanyProfile("GOOGL", "Alphabet Inc.", "tech", "ml"),
    # Cybersecurity / platform / cloud examples
    CompanyProfile("CRWD", "CrowdStrike Holdings, Inc.", "tech", "cybersec"),
    CompanyProfile("SNOW", "Snowflake Inc.", "tech", "database_as_service"),
    CompanyProfile("NET", "Cloudflare, Inc.", "tech", "paas"),
    # LiDAR-focused companies
    CompanyProfile("LAZR", "Luminar Technologies, Inc.", "tech", "lidar"),
    CompanyProfile("OUST", "Ouster, Inc.", "tech", "lidar"),
    CompanyProfile("AEVA", "Aeva Technologies, Inc.", "tech", "lidar"),
    # Quantum computing / adjacent
    CompanyProfile("IONQ", "IonQ, Inc.", "tech", "quantum_computing"),
    CompanyProfile("QBTS", "D-Wave Quantum Inc.", "tech", "quantum_computing"),
    CompanyProfile("QUBT", "Quantum Computing Inc.", "tech", "quantum_computing"),
    # Computer vision-related
    CompanyProfile("CGNX", "Cognex Corporation", "tech", "computer_vision"),
    CompanyProfile("MVIS", "MicroVision, Inc.", "tech", "computer_vision"),
]


def as_dicts() -> List[dict]:
    """Return configured company universe as serializable dictionaries."""
    return [asdict(company) for company in TARGET_COMPANIES]
