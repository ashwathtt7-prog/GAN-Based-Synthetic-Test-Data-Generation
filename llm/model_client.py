"""
Configurable LLM abstraction layer.
Routes to Gemini API or Ollama based on config.yaml.
The rest of the system never imports Gemini or Ollama directly — always uses ModelClient.
"""

import json
import yaml
import logging
from pathlib import Path
from typing import Type, Optional

from pydantic import BaseModel

logger = logging.getLogger(__name__)


def load_config() -> dict:
    """Load configuration from config.yaml."""
    config_path = Path(__file__).parent.parent / "config" / "config.yaml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


class ModelClient:
    """
    Unified LLM client that routes to Gemini or Ollama based on config.
    Single entry point: .invoke(prompt, output_schema) -> validated Pydantic model
    """

    def __init__(self, config: dict = None):
        self.config = config or load_config()
        self.llm_config = self.config["llm"]
        self.provider = self.llm_config["provider"]
        self.model_name = self.llm_config["model"]
        self.temperature = self.llm_config.get("temperature", 0.1)
        self.max_tokens = self.llm_config.get("max_tokens", 4096)
        self.retry_max = self.llm_config.get("retry_max", 3)
        self._client = None
        self._model = None

        self._initialize_client()

    def _initialize_client(self):
        """Initialize the appropriate LLM client based on provider config."""
        if self.provider == "gemini":
            self._init_gemini()
        elif self.provider == "ollama":
            self._init_ollama()
        else:
            raise ValueError(f"Unsupported LLM provider: {self.provider}")

    def _init_gemini(self):
        """Initialize Google Gemini client with service account credentials."""
        import google.generativeai as genai
        from google.oauth2 import service_account

        # Load service account credentials
        sa_path = self.llm_config.get("service_account_path")
        if sa_path:
            sa_full_path = Path(__file__).parent.parent / sa_path
            if sa_full_path.exists():
                credentials = service_account.Credentials.from_service_account_file(
                    str(sa_full_path),
                    scopes=["https://www.googleapis.com/auth/generative-language",
                            "https://www.googleapis.com/auth/cloud-platform"]
                )
                genai.configure(credentials=credentials)
                logger.info(f"Gemini initialized with service account: {sa_path}")
            else:
                # Fall back to default credentials / API key
                logger.warning(f"Service account not found at {sa_full_path}, using default auth")
                genai.configure()
        else:
            genai.configure()

        self._model = genai.GenerativeModel(
            model_name=self.model_name,
            generation_config=genai.GenerationConfig(
                temperature=self.temperature,
                max_output_tokens=self.max_tokens,
                response_mime_type="application/json",
            )
        )
        self._client = genai
        logger.info(f"Gemini model initialized: {self.model_name}")

    def _init_ollama(self):
        """Initialize Ollama client for local model inference."""
        import httpx

        base_url = self.llm_config.get("ollama_base_url", "http://localhost:11434")
        self._client = httpx.Client(base_url=base_url, timeout=120.0)
        self.model_name = self.llm_config.get("ollama_model", "gemma3:4b")
        logger.info(f"Ollama client initialized: {self.model_name} at {base_url}")

    def invoke(
        self,
        prompt: str,
        output_schema: Type[BaseModel],
        system_prompt: str = None,
        retry_on_failure: bool = True
    ) -> BaseModel:
        """
        Invoke the LLM with a prompt and validate output against a Pydantic schema.
        Retries up to retry_max times on validation failure with error feedback.

        Args:
            prompt: The user prompt to send
            output_schema: Pydantic model class to validate output against
            system_prompt: Optional system prompt override
            retry_on_failure: Whether to retry on validation failure

        Returns:
            Validated Pydantic model instance
        """
        if system_prompt is None:
            system_prompt = self._default_system_prompt()

        # Add schema specification to prompt
        schema_json = json.dumps(output_schema.model_json_schema(), indent=2)
        full_prompt = (
            f"{system_prompt}\n\n"
            f"Output JSON Schema (you MUST conform to this exactly):\n"
            f"```json\n{schema_json}\n```\n\n"
            f"{prompt}"
        )

        last_error = None
        for attempt in range(self.retry_max if retry_on_failure else 1):
            try:
                if attempt > 0:
                    full_prompt += (
                        f"\n\nPrevious attempt {attempt} failed validation with error:\n"
                        f"{last_error}\n"
                        f"Please fix the output to conform to the schema exactly."
                    )

                raw_response = self._call_provider(full_prompt)
                parsed = self._parse_json_response(raw_response)
                validated = output_schema.model_validate(parsed)

                logger.info(
                    f"LLM call succeeded on attempt {attempt + 1} "
                    f"for schema {output_schema.__name__}"
                )
                return validated

            except Exception as e:
                last_error = str(e)
                logger.warning(
                    f"LLM call attempt {attempt + 1}/{self.retry_max} failed: {e}"
                )

        raise ValueError(
            f"LLM failed to produce valid {output_schema.__name__} "
            f"after {self.retry_max} attempts. Last error: {last_error}"
        )

    def invoke_raw(self, prompt: str, system_prompt: str = None) -> str:
        """Invoke the LLM and return raw text response (no schema validation)."""
        if system_prompt is None:
            system_prompt = self._default_system_prompt()

        full_prompt = f"{system_prompt}\n\n{prompt}"
        return self._call_provider(full_prompt)

    def invoke_batch(
        self,
        prompts: list[str],
        output_schema: Type[BaseModel],
        system_prompt: str = None
    ) -> list[BaseModel]:
        """Invoke the LLM for multiple prompts sequentially."""
        results = []
        for prompt in prompts:
            result = self.invoke(prompt, output_schema, system_prompt)
            results.append(result)
        return results

    def _call_provider(self, prompt: str) -> str:
        """Route to the appropriate provider's API."""
        if self.provider == "gemini":
            return self._call_gemini(prompt)
        elif self.provider == "ollama":
            return self._call_ollama(prompt)
        else:
            raise ValueError(f"Unsupported provider: {self.provider}")

    def _call_gemini(self, prompt: str) -> str:
        """Call Gemini API and return text response."""
        response = self._model.generate_content(prompt)
        return response.text

    def _call_ollama(self, prompt: str) -> str:
        """Call Ollama REST API and return text response."""
        payload = {
            "model": self.model_name,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": self.temperature,
                "num_predict": self.max_tokens,
            },
            "format": "json"
        }
        response = self._client.post("/api/generate", json=payload)
        response.raise_for_status()
        return response.json()["response"]

    def _parse_json_response(self, raw: str) -> dict:
        """Parse JSON from LLM response, handling markdown code blocks."""
        text = raw.strip()

        # Strip markdown code blocks if present
        if text.startswith("```json"):
            text = text[7:]
        elif text.startswith("```"):
            text = text[3:]
        if text.endswith("```"):
            text = text[:-3]

        text = text.strip()
        return json.loads(text)

    def _default_system_prompt(self) -> str:
        """Default system prompt for all LLM calls."""
        return (
            "You are a data engineering specialist with deep expertise in telecom "
            "business systems and enterprise database schemas. You reason carefully "
            "about what data means in business context, not just what it looks like "
            "syntactically.\n\n"
            "You have access to tools to traverse a Neo4j knowledge graph. Always use "
            "these tools to gather context before making decisions. Never assume — "
            "always verify by querying the graph.\n\n"
            "You output ONLY valid JSON conforming to the schema you are given. No "
            "preamble, no explanation outside the JSON. If you are uncertain, express "
            "that uncertainty as a low llm_confidence score and a detailed notes field.\n\n"
            "Security constraint: You are working with metadata and statistics only. "
            "You never see real customer values. Sample values shown to you have been "
            "pre-screened by Presidio. Do not attempt to reconstruct or identify real "
            "individuals from any data shown to you."
        )


# Singleton instance for convenience
_client_instance: Optional[ModelClient] = None


def get_model_client(config: dict = None) -> ModelClient:
    """Get or create a singleton ModelClient instance."""
    global _client_instance
    if _client_instance is None:
        _client_instance = ModelClient(config)
    return _client_instance
