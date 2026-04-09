"""
Presidio Scanner
Detects PII deterministically before LLM reasoning.
Implements built-in and custom telecom recognizers.
"""

import logging
import spacy
from presidio_analyzer import AnalyzerEngine, RecognizerRegistry, PatternRecognizer, Pattern
from presidio_analyzer.nlp_engine import NlpEngineProvider
from models.schemas import PresidioResult

logger = logging.getLogger(__name__)

class PresidioScanner:
    def __init__(self, config: dict):
        self.config = config.get("presidio", {})
        self.threshold = self.config.get("confidence_threshold", 0.7)
        self.enabled_entities = self.config.get("enabled_recognizers", ["PERSON", "EMAIL_ADDRESS", "PHONE_NUMBER"])
        
        # Initialize analyzer
        self.registry = RecognizerRegistry()
        self.registry.load_predefined_recognizers()
        
        # Add custom telecom recognizers
        self._add_custom_recognizers()
        
        nlp_engine = self._build_nlp_engine()
        self.analyzer = AnalyzerEngine(
            registry=self.registry,
            nlp_engine=nlp_engine,
            supported_languages=["en"]
        )

    def _build_nlp_engine(self):
        """
        Build a Presidio NLP engine using the configured spaCy model, with a
        local fallback to a smaller installed model to avoid runtime downloads.
        """
        configured_model = self.config.get("spacy_model", "en_core_web_lg")
        fallback_models = [configured_model, "en_core_web_sm"]
        chosen_model = None

        for model_name in fallback_models:
            if spacy.util.is_package(model_name):
                chosen_model = model_name
                break

        if chosen_model is None:
            raise RuntimeError(
                "No compatible spaCy model is installed for Presidio. "
                f"Tried: {', '.join(dict.fromkeys(fallback_models))}"
            )

        if chosen_model != configured_model:
            logger.warning(
                "Configured spaCy model '%s' is unavailable. Falling back to '%s'.",
                configured_model,
                chosen_model,
            )

        provider = NlpEngineProvider(
            nlp_configuration={
                "nlp_engine_name": "spacy",
                "models": [{"lang_code": "en", "model_name": chosen_model}],
            }
        )
        return provider.create_engine()

    def _add_custom_recognizers(self):
        """Build and register custom telecom pattern recognizers."""
        custom_entities = self.config.get("custom_recognizers", [])
        
        if "IMSI" in custom_entities:
            imsi_pattern = Pattern(name="imsi_pattern", regex=r"\b\d{15}\b", score=0.85)
            imsi_recognizer = PatternRecognizer(
                supported_entity="IMSI",
                patterns=[imsi_pattern],
                context=["imsi", "sim", "subscriber"]
            )
            self.registry.add_recognizer(imsi_recognizer)
            if "IMSI" not in self.enabled_entities:
                self.enabled_entities.append("IMSI")
                
        if "SUBSCRIBER_ID" in custom_entities:
            # Synthetic telecom pattern: e.g. SUB-12345678
            sub_pattern = Pattern(name="sub_pattern", regex=r"\bSUB-\d{8}\b", score=0.9)
            sub_recognizer = PatternRecognizer(
                supported_entity="SUBSCRIBER_ID",
                patterns=[sub_pattern],
                context=["subscriber", "account"]
            )
            self.registry.add_recognizer(sub_recognizer)
            if "SUBSCRIBER_ID" not in self.enabled_entities:
                self.enabled_entities.append("SUBSCRIBER_ID")
                
        # More custom recognizers for Network Element ID, etc. could be added here
        logger.info("Custom telecom recognizers loaded into Presidio.")

    def scan_column(self, table_name: str, column_name: str, sample_values: list[str]) -> PresidioResult:
        """Scan a sample of values from a column to determine if it contains PII."""
        
        result = PresidioResult(
            table_name=table_name,
            column_name=column_name
        )
        
        if not sample_values:
            return result

        matches = []
        highest_confidence = 0.0
        dominant_entity = None
        
        # We run the analyzer on a sample of values
        for val in sample_values:
            if not isinstance(val, str) or val in ("None", "NaN", "null", ""):
                 continue
                 
            results = self.analyzer.analyze(
                text=val,
                entities=self.enabled_entities,
                language="en"
            )
            
            for res in results:
                if res.score >= self.threshold:
                    if res.score > highest_confidence:
                        highest_confidence = res.score
                        dominant_entity = res.entity_type
                    # Keep a few examples for context
                    if len(matches) < 3:
                         matches.append(val)

        if dominant_entity:
            result.pii_detected = True
            result.pii_type = dominant_entity
            result.confidence = highest_confidence
            result.sample_matches = matches
            
        return result
        
    def is_pii_passthrough(self, pii_type: str) -> str:
        """Determine default masking strategy based on PII type."""
        # This acts as a fast-path for deterministic masking
        # E.g., Phone numbers get substitute_realistic, internal IDs get format_preserving
        if pii_type in ["PERSON", "EMAIL_ADDRESS", "PHONE_NUMBER", "LOCATION"]:
            return "substitute_realistic"
        elif pii_type in ["CREDIT_CARD", "SSN", "IMSI", "SUBSCRIBER_ID"]:
             return "format_preserving"
        elif pii_type in ["IP_ADDRESS", "URL", "DATE_TIME"]:
             return "substitute_realistic"
        return "suppress"
