"""
Pre-generation Masking Engine (Step 3.2)
Applies masking strategies to real data BEFORE passing to CTGAN/TVAE training.
CTGAN/TVAE trains ONLY on masked data — never on raw PII.
"""

import logging
import re
import random
from faker import Faker
import pandas as pd

logger = logging.getLogger(__name__)


class MaskingEngine:
    def __init__(self, locale: str = "en_US"):
        self.fake = Faker(locale)

    def mask_dataframe(self, df: pd.DataFrame, column_policies: list) -> pd.DataFrame:
        """
        Apply masking strategies to all columns based on LLM/Presidio policies.
        Returns a masked copy — original data is never modified.

        Args:
            df: Real source data
            column_policies: List of ColumnPolicySchema (or dicts with masking_strategy)

        Returns:
            Masked DataFrame safe for GAN training
        """
        masked_df = df.copy()
        policy_map = {}
        for p in column_policies:
            name = p.column_name if hasattr(p, 'column_name') else p.get('column_name')
            policy_map[name] = p

        for col in masked_df.columns:
            policy = policy_map.get(col)
            if not policy:
                continue

            strategy = policy.masking_strategy if hasattr(policy, 'masking_strategy') else policy.get('masking_strategy', 'passthrough')

            if strategy == "passthrough":
                continue
            elif strategy == "suppress":
                masked_df = masked_df.drop(columns=[col])
                logger.info(f"[Masking] Suppressed column: {col}")
            elif strategy == "substitute_realistic":
                masked_df[col] = self._substitute_realistic(masked_df[col], col, policy)
                logger.info(f"[Masking] Substituted realistic values for: {col}")
            elif strategy == "format_preserving":
                masked_df[col] = self._format_preserving(masked_df[col], col, policy)
                logger.info(f"[Masking] Format-preserving mask for: {col}")
            elif strategy == "generalise":
                masked_df[col] = self._generalise(masked_df[col], col, policy)
                logger.info(f"[Masking] Generalised column: {col}")

        return masked_df

    def _substitute_realistic(self, series: pd.Series, col_name: str, policy) -> pd.Series:
        """Replace values with realistic Faker-generated substitutes."""
        pii_type = None
        if hasattr(policy, 'pii_classification'):
            pii_type = policy.pii_classification
        col_upper = col_name.upper()

        def generate_value():
            if "FRST_NM" in col_upper or "FIRST" in col_upper:
                return self.fake.first_name()
            elif "LST_NM" in col_upper or "LAST" in col_upper:
                return self.fake.last_name()
            elif "MID_NM" in col_upper or "MIDDLE" in col_upper:
                return self.fake.first_name()
            elif "EMAIL" in col_upper:
                return self.fake.email()
            elif "PHONE" in col_upper or "MSISDN" in col_upper:
                return self.fake.phone_number()[:15]
            elif "ADDR" in col_upper and "LN" in col_upper:
                return self.fake.street_address()
            elif "CITY" in col_upper:
                return self.fake.city()
            elif "ST_CD" in col_upper or "STATE" in col_upper:
                return self.fake.state_abbr()
            elif "ZIP" in col_upper:
                return self.fake.zipcode()
            elif "IP" in col_upper:
                return self.fake.ipv4()
            elif "URL" in col_upper:
                return self.fake.url()
            elif "NM" in col_upper or "NAME" in col_upper:
                return self.fake.name()
            else:
                return self.fake.bothify('????####')

        return series.apply(lambda x: generate_value() if pd.notna(x) else None)

    def _format_preserving(self, series: pd.Series, col_name: str, policy) -> pd.Series:
        """Replace values maintaining the same format/regex pattern."""
        col_upper = col_name.upper()
        constraint = {}
        if hasattr(policy, 'constraint_profile'):
            constraint = policy.constraint_profile or {}
        elif isinstance(policy, dict):
            constraint = policy.get('constraint_profile', {})

        regex_pattern = constraint.get('regex')

        def mask_value(val):
            if pd.isna(val):
                return None
            s = str(val)
            if "SSN" in col_upper:
                return str(random.randint(100000000, 999999999))
            elif "IMSI" in col_upper:
                return str(random.randint(100000000000000, 999999999999999))
            elif "ICCID" in col_upper:
                return str(random.randint(10**19, 10**20 - 1))
            elif "CREDIT_CARD" in col_upper or "CC" in col_upper:
                return str(random.randint(1000000000000000, 9999999999999999))
            else:
                # Preserve character types: digits→digits, letters→letters
                result = []
                for c in s:
                    if c.isdigit():
                        result.append(str(random.randint(0, 9)))
                    elif c.isalpha():
                        result.append(random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ') if c.isupper() else random.choice('abcdefghijklmnopqrstuvwxyz'))
                    else:
                        result.append(c)
                return ''.join(result)

        return series.apply(mask_value)

    def _generalise(self, series: pd.Series, col_name: str, policy) -> pd.Series:
        """Replace with range bucket or category label."""
        if series.dtype in ('int64', 'float64'):
            # Bucket into ranges
            def bucket(val):
                if pd.isna(val):
                    return None
                v = float(val)
                if v < 0:
                    return "NEGATIVE"
                elif v < 25:
                    return "0-25"
                elif v < 50:
                    return "25-50"
                elif v < 75:
                    return "50-75"
                elif v < 100:
                    return "75-100"
                else:
                    return "100+"
            return series.apply(bucket)
        else:
            # For strings, replace with generic category
            return series.apply(lambda x: "REDACTED" if pd.notna(x) else None)
