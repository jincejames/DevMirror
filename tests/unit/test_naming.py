"""Tests for devmirror.utils.naming.

Golden tests are derived from SPECIFICATION.md sections 3.2.1 and 4.1 examples:
  PROD: prod_analytics.customers       -> DEV: dev_analytics.dr_1042_customers
  PROD: prod_analytics.customers       -> QA:  dev_analytics.qa_1042_customers
  Object: prod_analytics.customers.churn_scores -> dev_analytics.dr_1042_customers.churn_scores
"""

from __future__ import annotations

import pytest

from devmirror.utils.naming import (
    NamingError,
    dev_schema_prefix,
    extract_dr_number,
    qa_schema_prefix,
    required_target_schemas,
    schema_prefix,
    target_object_fqn,
    target_schema_fqn,
)

# ===========================================================================
# extract_dr_number
# ===========================================================================

class TestExtractDrNumber:
    @pytest.mark.parametrize(
        ("dr_id", "expected"),
        [
            ("DR-0", "0"),
            ("DR-1", "1"),
            ("DR-1042", "1042"),
            ("DR-999999", "999999"),
        ],
    )
    def test_valid(self, dr_id: str, expected: str) -> None:
        assert extract_dr_number(dr_id) == expected

    @pytest.mark.parametrize(
        "bad_id",
        ["DR-", "dr-100", "DR100", "WR-100", "DR-abc", "", "DR-12-34"],
    )
    def test_invalid(self, bad_id: str) -> None:
        with pytest.raises(NamingError, match="Invalid dr_id format"):
            extract_dr_number(bad_id)


# ===========================================================================
# dev_schema_prefix / qa_schema_prefix / schema_prefix
# ===========================================================================

class TestPrefixes:
    def test_dev_prefix(self) -> None:
        assert dev_schema_prefix("DR-1042") == "dr_1042"

    def test_qa_prefix(self) -> None:
        assert qa_schema_prefix("DR-1042") == "qa_1042"

    def test_dev_prefix_zero(self) -> None:
        assert dev_schema_prefix("DR-0") == "dr_0"

    def test_qa_prefix_large(self) -> None:
        assert qa_schema_prefix("DR-999999") == "qa_999999"

    def test_schema_prefix_dev(self) -> None:
        assert schema_prefix("DR-42", "dev") == "dr_42"

    def test_schema_prefix_qa(self) -> None:
        assert schema_prefix("DR-42", "qa") == "qa_42"

    def test_schema_prefix_bad_env(self) -> None:
        with pytest.raises(NamingError, match="Unknown environment"):
            schema_prefix("DR-42", "staging")  # type: ignore[arg-type]


# ===========================================================================
# target_schema_fqn - golden tests from SPECIFICATION.md
# ===========================================================================

class TestTargetSchemaFqn:
    def test_spec_example_dev(self) -> None:
        """SPEC 3.2.1: prod_analytics.customers -> dev_analytics.dr_1042_customers"""
        result = target_schema_fqn("dev_analytics", "prod_analytics.customers", "DR-1042", "dev")
        assert result == "dev_analytics.dr_1042_customers"

    def test_spec_example_qa(self) -> None:
        """SPEC 3.2.1: prod_analytics.customers -> dev_analytics.qa_1042_customers"""
        result = target_schema_fqn("dev_analytics", "prod_analytics.customers", "DR-1042", "qa")
        assert result == "dev_analytics.qa_1042_customers"

    def test_three_part_input_uses_schema_only(self) -> None:
        """When given a three-part FQN, only catalog.schema is used."""
        result = target_schema_fqn(
            "dev_analytics", "prod_analytics.customers.some_table", "DR-1", "dev"
        )
        assert result == "dev_analytics.dr_1_customers"

    def test_single_part_rejected(self) -> None:
        with pytest.raises(NamingError, match="at least 2"):
            target_schema_fqn("dev_analytics", "customers", "DR-1", "dev")

    def test_different_catalogs(self) -> None:
        result = target_schema_fqn("qa_catalog", "prod.schema_x", "DR-500", "qa")
        assert result == "qa_catalog.qa_500_schema_x"


# ===========================================================================
# target_object_fqn - golden tests
# ===========================================================================

class TestTargetObjectFqn:
    def test_spec_example(self) -> None:
        """SPEC: prod_analytics.customers.churn_scores -> dev_analytics.dr_1042_customers.churn_scores"""
        result = target_object_fqn(
            "dev_analytics", "prod_analytics.customers.churn_scores", "DR-1042", "dev"
        )
        assert result == "dev_analytics.dr_1042_customers.churn_scores"

    def test_qa_object(self) -> None:
        result = target_object_fqn(
            "dev_analytics", "prod_analytics.shared.date_dim", "DR-1042", "qa"
        )
        assert result == "dev_analytics.qa_1042_shared.date_dim"

    def test_two_part_fqn_rejected(self) -> None:
        with pytest.raises(NamingError, match="three-part"):
            target_object_fqn("dev", "catalog.schema", "DR-1", "dev")

    def test_four_part_fqn_rejected(self) -> None:
        with pytest.raises(NamingError, match="three-part"):
            target_object_fqn("dev", "a.b.c.d", "DR-1", "dev")


# ===========================================================================
# required_target_schemas
# ===========================================================================

class TestRequiredTargetSchemas:
    def test_deduplication(self) -> None:
        result = required_target_schemas(
            "dev_analytics",
            ["prod.customers", "prod.customers", "prod.shared"],
            "DR-1042",
            "dev",
        )
        assert result == [
            "dev_analytics.dr_1042_customers",
            "dev_analytics.dr_1042_shared",
        ]

    def test_sorted_output(self) -> None:
        result = required_target_schemas(
            "dev",
            ["cat.zzz", "cat.aaa", "cat.mmm"],
            "DR-1",
            "dev",
        )
        assert result == ["dev.dr_1_aaa", "dev.dr_1_mmm", "dev.dr_1_zzz"]

    def test_empty_list(self) -> None:
        assert required_target_schemas("dev", [], "DR-1", "dev") == []

    def test_qa_environment(self) -> None:
        result = required_target_schemas(
            "qa_cat", ["prod.schema_a"], "DR-77", "qa"
        )
        assert result == ["qa_cat.qa_77_schema_a"]
