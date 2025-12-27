"""Configuration loading and validation for SEC digest extraction."""

from pathlib import Path
from typing import Literal
import yaml
from pydantic import BaseModel, Field
from dotenv import load_dotenv
import os


class LLMConfig(BaseModel):
    """LLM provider configuration."""
    provider: Literal["anthropic", "openai", "ollama"] = "ollama"
    model: str = "llama3.1"
    temperature: float = 0.0
    host: str = "http://localhost:11434"  # For Ollama


class ScraperConfig(BaseModel):
    """Web scraper configuration."""
    delay_seconds: int = Field(default=2, ge=1)
    max_retries: int = Field(default=3, ge=1)
    start_year: int = Field(default=1985, ge=1956, le=2014)
    end_year: int = Field(default=1985, ge=1956, le=2014)


class PathsConfig(BaseModel):
    """Data paths configuration."""
    raw_data: Path = Path("data/raw")
    markdown: Path = Path("data/markdown")
    extracted: Path = Path("data/extracted")
    database: Path = Path("data/processed/sec_digest.duckdb")


class Config(BaseModel):
    """Main application configuration."""
    llm: LLMConfig
    scraper: ScraperConfig
    paths: PathsConfig

    @classmethod
    def load(cls, config_path: str = "config.yaml") -> "Config":
        """Load configuration from YAML file and environment variables."""
        # Load environment variables
        load_dotenv()

        # Load YAML config
        with open(config_path) as f:
            config_dict = yaml.safe_load(f)

        # Override with environment variables if present
        if os.getenv("OLLAMA_HOST"):
            config_dict.setdefault("llm", {})["host"] = os.getenv("OLLAMA_HOST")
        if os.getenv("OLLAMA_MODEL"):
            config_dict.setdefault("llm", {})["model"] = os.getenv("OLLAMA_MODEL")

        return cls(**config_dict)

    def ensure_directories(self) -> None:
        """Create all necessary directories if they don't exist."""
        for path in [self.paths.raw_data, self.paths.markdown,
                     self.paths.extracted, self.paths.database.parent]:
            path.mkdir(parents=True, exist_ok=True)
