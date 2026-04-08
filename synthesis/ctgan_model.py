"""
CTGAN Model Adapter
Wraps SDV's CTGAN for synthetic data generation based on LLM policies.
"""

import logging
import os
import pandas as pd
from sdv.single_table import CTGANSynthesizer
from sdv.metadata import SingleTableMetadata

logger = logging.getLogger(__name__)

class CTGANModel:
    def __init__(self, table_name: str, policies: list):
        self.table_name = table_name
        self.policies = {p.column_name: p for p in policies}
        self.metadata = SingleTableMetadata()
        self.synthesizer = None
        
    def _detect_sdv_type(self, col_name: str, data_type: str, policy) -> dict:
        """Map generic SQL types and LLM policy to SDV metadata."""
        sdv_meta = {"type": "categorical"}
        
        # Override with Presidio/LLM masking hints
        if policy and policy.masking_strategy == "format_preserving":
             if policy.pii_classification == "SSN" or "SSN" in col_name:
                 return {"type": "id"}
                 
        if "DATE" in data_type.upper() or "DATETIME" in data_type.upper():
             return {"type": "datetime", "format": "%Y-%m-%d %H:%M:%S"}
             
        if "INT" in data_type.upper() or "DEC" in data_type.upper() or "NUM" in data_type.upper():
             if policy and policy.constraint_profile.get("pattern_type") != "finite_categorical":
                 return {"type": "numerical"}
                 
        if "VARCHAR" in data_type.upper():
             if "ID" in col_name and (not policy or policy.dedup_mode == "entity"):
                  return {"type": "id", "subtype": "string"}
                  
        return sdv_meta

    def _build_sdv_metadata(self, df: pd.DataFrame):
        """Construct SDV Metadata dynamically from dataframe and LLM policy context."""
        meta_dict = {"columns": {}}
        
        # Primary key assumption
        pk_col = f"{self.table_name.replace('_MSTR', '').replace('_ACCT', '')}_ID"
        if pk_col in df.columns:
            meta_dict["primary_key"] = pk_col
            
        for col in df.columns:
            # We skip generating completely suppressed columns
            policy = self.policies.get(col)
            if policy and policy.masking_strategy == "suppress":
                 continue
                 
            # Infer Type
            meta_dict["columns"][col] = self._detect_sdv_type(
                 col_name=col,
                 data_type=str(df[col].dtype), # Simple proxy for type
                 policy=policy
            )
            
        self.metadata.detect_from_dataframe(data=df)
        # Update metadata with our manual hints
        try:
           self.metadata.update_columns(column_metadata=meta_dict["columns"])
        except Exception as e:
           logger.warning(f"Could not apply all manual SDV metadata hints: {e}")
           
        logger.info(f"SDV Metadata build complete for {self.table_name}")

    def train(self, df: pd.DataFrame, epochs: int = 150):
        """Train the CTGAN synthesizer."""
        logger.info(f"Training CTGAN on {self.table_name} with {len(df)} records for {epochs} epochs...")
        
        # Drop suppressed columns before training
        drop_cols = [c for c, p in self.policies.items() if p.masking_strategy == "suppress"]
        train_df = df.drop(columns=[c for c in drop_cols if c in df.columns])
        
        self._build_sdv_metadata(train_df)
        
        self.synthesizer = CTGANSynthesizer(self.metadata, epochs=epochs)
        
        # Incorporate LLM boundary constraints conceptually into bounds/distributions 
        # (SDV does this somewhat automatically with numerical types, but we could enforce ranges)
        
        self.synthesizer.fit(train_df)
        logger.info(f"CTGAN Model trained for {self.table_name}.")

    def generate(self, num_rows: int) -> pd.DataFrame:
        """Sample synthetic row data."""
        if not self.synthesizer:
            raise ValueError("Synthesizer not trained yet.")
            
        logger.info(f"Generating {num_rows} synthetic records for {self.table_name}...")
        synthetic_data = self.synthesizer.sample(num_rows=num_rows)
        return synthetic_data

    def save(self, output_dir: str):
        """Persist model to disk."""
        if self.synthesizer:
             os.makedirs(output_dir, exist_ok=True)
             self.synthesizer.save(f"{output_dir}/{self.table_name}_ctgan.pkl")
