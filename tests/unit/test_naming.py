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
    import_schema_fqn,
    qa_schema_prefix,
    required_target_schemas,
    resolve_target_catalog,
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
            # Legacy hyphen-separated form
            ("DR-0", "0"),
            ("DR-1", "1"),
            ("DR-1042", "1042"),
            ("DR-999999", "999999"),
            # Stage 4 auto-generated form (no hyphen, zero-padded counter)
            ("DR00001", "00001"),
            ("DR12345", "12345"),
            ("DR000000001", "000000001"),
            ("PROJ00042", "00042"),       # custom prefix
            ("ABC100", "100"),             # 3-digit counter is the minimum
        ],
    )
    def test_valid(self, dr_id: str, expected: str) -> None:
        assert extract_dr_number(dr_id) == expected

    @pytest.mark.parametrize(
        "bad_id",
        [
            "DR-",          # legacy form needs digits after the hyphen
            "DR-abc",       # legacy form requires digits
            "DR-12-34",     # double hyphen not in either form
            "WR-100",       # legacy form requires literal 'DR-' prefix
            "DR1",          # new form requires at least 3 digits
            "DR12",         # new form requires at least 3 digits
            "1DR123",       # new form prefix must start with a letter
            "",             # empty string
        ],
    )
    def test_invalid(self, bad_id: str) -> None:
        with pytest.raises(NamingError, match="matches neither"):
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


# ===========================================================================
# resolve_target_catalog (LH SDLC suffix scheme)
# ===========================================================================

class TestResolveTargetCatalog:
    """Per-object suffix-based catalog routing.

    The source's SDLC suffix (`_p`/`_i`/`_n`) is stripped to recover the
    base name; the env-specific suffix (`_n` for dev, `_i` for qa) is
    re-attached.  Different objects in one DR can have different bases.
    """

    def test_prod_to_dev(self) -> None:
        assert (
            resolve_target_catalog("odp_adw_ancillaries_p", "dev")
            == "odp_adw_ancillaries_n"
        )

    def test_prod_to_qa(self) -> None:
        assert (
            resolve_target_catalog("odp_adw_ancillaries_p", "qa")
            == "odp_adw_ancillaries_i"
        )

    def test_dev_to_dev_idempotent(self) -> None:
        # Source already in the dev catalog -- target is the same.
        assert (
            resolve_target_catalog("odp_adw_ancillaries_n", "dev")
            == "odp_adw_ancillaries_n"
        )

    def test_qa_source_rerouted_to_dev(self) -> None:
        # Source from QA being re-cloned for a dev DR.
        assert (
            resolve_target_catalog("odp_adw_ancillaries_i", "dev")
            == "odp_adw_ancillaries_n"
        )

    def test_no_known_suffix_treats_full_name_as_base(self) -> None:
        # No `_p`/`_i`/`_n` suffix -- whole name is the base, suffix appended.
        assert resolve_target_catalog("foo", "dev") == "foo_n"
        assert resolve_target_catalog("foo", "qa") == "foo_i"

    def test_multiple_underscore_segments_only_strips_known_suffix(self) -> None:
        # `odp_adw_ancillaries` has many underscores; only the trailing
        # `_p` is the SDLC suffix -- the rest of the name is intact.
        assert (
            resolve_target_catalog("odp_adw_ancillaries_p", "dev")
            == "odp_adw_ancillaries_n"
        )
        assert (
            resolve_target_catalog("odp_adw_offers_p", "dev")
            == "odp_adw_offers_n"
        )

    def test_different_objects_different_bases(self) -> None:
        # Two source catalogs with different base names map to two different
        # target catalogs in the same env -- this is the whole point of
        # per-object resolution.
        assert (
            resolve_target_catalog("odp_adw_ancillaries_p", "dev")
            == "odp_adw_ancillaries_n"
        )
        assert (
            resolve_target_catalog("odp_adw_offers_p", "dev")
            == "odp_adw_offers_n"
        )

    def test_dev_suffix_env_var_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DEVMIRROR_DEV_CATALOG_SUFFIX", "_dev")
        assert (
            resolve_target_catalog("odp_adw_ancillaries_p", "dev")
            == "odp_adw_ancillaries_dev"
        )

    def test_qa_suffix_env_var_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DEVMIRROR_QA_CATALOG_SUFFIX", "_qa")
        assert (
            resolve_target_catalog("odp_adw_ancillaries_p", "qa")
            == "odp_adw_ancillaries_qa"
        )

    def test_explicit_target_catalog_override_wins(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # When DEVMIRROR_TARGET_CATALOG is set (typically via the per-DR
        # ConfigIn.target_catalog -> _target_catalog_override path), it
        # short-circuits the suffix logic for every object.
        monkeypatch.setenv("DEVMIRROR_TARGET_CATALOG", "odp_adw_sandbox_n")
        assert (
            resolve_target_catalog("odp_adw_ancillaries_p", "dev")
            == "odp_adw_sandbox_n"
        )
        assert (
            resolve_target_catalog("odp_adw_offers_p", "dev")
            == "odp_adw_sandbox_n"
        )
        # Even for QA -- a manual override is always literal.
        assert (
            resolve_target_catalog("odp_adw_ancillaries_p", "qa")
            == "odp_adw_sandbox_n"
        )

    def test_empty_override_falls_through_to_suffix_logic(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # An empty/whitespace DEVMIRROR_TARGET_CATALOG must NOT be treated
        # as an override -- it should fall through to suffix routing.
        monkeypatch.setenv("DEVMIRROR_TARGET_CATALOG", "   ")
        assert (
            resolve_target_catalog("odp_adw_ancillaries_p", "dev")
            == "odp_adw_ancillaries_n"
        )

    def test_empty_suffix_env_vars_fall_back_to_defaults(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Bundle / app.yaml configs commonly use `value: ""` to mean "unset".
        # If we treated those as authoritative we'd silently produce
        # suffix-less catalog names.  Empty / whitespace must fall back to
        # the LH defaults.
        monkeypatch.setenv("DEVMIRROR_PROD_CATALOG_SUFFIX", "")
        monkeypatch.setenv("DEVMIRROR_QA_CATALOG_SUFFIX", "   ")
        monkeypatch.setenv("DEVMIRROR_DEV_CATALOG_SUFFIX", "")
        assert (
            resolve_target_catalog("odp_adw_ancillaries_p", "dev")
            == "odp_adw_ancillaries_n"
        )
        assert (
            resolve_target_catalog("odp_adw_ancillaries_p", "qa")
            == "odp_adw_ancillaries_i"
        )

    def test_unknown_env_raises(self) -> None:
        # Defensive: anything other than "dev" / "qa" must NOT silently
        # route to QA -- it has to surface as a NamingError so the caller
        # sees the bug instead of finding clones in the wrong catalog.
        with pytest.raises(NamingError, match="Unknown environment"):
            resolve_target_catalog("odp_adw_ancillaries_p", "prod")
        with pytest.raises(NamingError, match="Unknown environment"):
            resolve_target_catalog("odp_adw_ancillaries_p", "")

    def test_prod_suffix_env_var_override_extends_known_set(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # If a customer uses `_prod` instead of `_p`, configuring the env
        # var must make the function strip `_prod` from the source.
        monkeypatch.setenv("DEVMIRROR_PROD_CATALOG_SUFFIX", "_prod")
        assert (
            resolve_target_catalog("foo_prod", "dev") == "foo_n"
        )
        # And `_p` is no longer treated as a known suffix in this config,
        # so a `_p`-suffixed source falls through to "no known suffix"
        # (full name as base) -- documents the contract.
        assert resolve_target_catalog("foo_p", "dev") == "foo_p_n"

    def test_lh_qa_lands_in_dev_but_strips_integration_source(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # The LH-specific bug:  DEVMIRROR_QA_CATALOG_SUFFIX=_n collapses the
        # landing trio to (_p, _n, _n), which removes `_i` from the strip set
        # and causes pre-prod sources (ending `_i`) to be re-suffixed instead
        # of re-routed.  DEVMIRROR_KNOWN_CATALOG_SUFFIXES restores `_i` to
        # the recognition set without changing where QA lands.
        monkeypatch.setenv("DEVMIRROR_QA_CATALOG_SUFFIX", "_n")
        monkeypatch.setenv("DEVMIRROR_KNOWN_CATALOG_SUFFIXES", "_p,_i,_n")
        assert (
            resolve_target_catalog("odp_adw_revenue_management_i", "dev")
            == "odp_adw_revenue_management_n"
        )
        # QA landing also goes into _n in this config (the LH convention).
        assert (
            resolve_target_catalog("odp_adw_revenue_management_i", "qa")
            == "odp_adw_revenue_management_n"
        )

    def test_known_suffixes_override_explicit_only(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # When DEVMIRROR_KNOWN_CATALOG_SUFFIXES is set, the landing trio is
        # NOT auto-unioned in -- the override is authoritative.  Forces the
        # operator to keep the recognition set in sync with reality.
        monkeypatch.setenv("DEVMIRROR_KNOWN_CATALOG_SUFFIXES", "_p")
        # `_n` source has no recognised suffix -> base = full name, landing
        # appends `_n` -> `foo_n_n`.
        assert resolve_target_catalog("foo_n", "dev") == "foo_n_n"

    def test_known_suffixes_empty_falls_back_to_landing_trio(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Empty / whitespace value must behave as if unset (same convention
        # as the other suffix env vars).
        monkeypatch.setenv("DEVMIRROR_KNOWN_CATALOG_SUFFIXES", "   ")
        assert (
            resolve_target_catalog("odp_adw_ancillaries_p", "dev")
            == "odp_adw_ancillaries_n"
        )


# ===========================================================================
# import_schema_fqn (per-DR import schema for sideloaded artifacts)
# ===========================================================================

class TestImportSchemaFqn:
    def test_dev(self) -> None:
        assert (
            import_schema_fqn("odp_adw_ancillaries_n", "DR-1042", "dev", "import_main")
            == "odp_adw_ancillaries_n.dr_1042_import_main"
        )

    def test_qa(self) -> None:
        assert (
            import_schema_fqn("odp_adw_ancillaries_n", "DR-1042", "qa", "import_main")
            == "odp_adw_ancillaries_n.qa_1042_import_main"
        )

    def test_zero_padded_dr_id(self) -> None:
        # Stage-4 auto-generated form keeps the zero-pad so schema names
        # sort lexically alongside the dr_<num>_<schema> targets.
        assert (
            import_schema_fqn("cat", "DR00042", "dev", "import_main")
            == "cat.dr_00042_import_main"
        )

    def test_custom_suffix(self) -> None:
        # The suffix is a deploy-time config knob; the helper is suffix-
        # agnostic.
        assert (
            import_schema_fqn("cat", "DR-1", "dev", "sideload")
            == "cat.dr_1_sideload"
        )

    def test_unknown_env_raises(self) -> None:
        with pytest.raises(NamingError, match="Unknown environment"):
            import_schema_fqn("cat", "DR-1", "prod", "import_main")  # type: ignore[arg-type]
