import pandas as pd
from src.utils.io_utils import read_input_file
from src.cleaning import apply_ingest_mapping

def test_ingest_minimal_name_only(tmp_path, monkeypatch):
    csv = tmp_path / "in.csv"
    csv.write_text("Name,Notes\nAcme Inc,foo\nBeta Co,bar\n")
    df = read_input_file(str(csv))

    # fixed run_id for deterministic internal_row_id
    out = apply_ingest_mapping(
        df,
        name_col="Name",
        id_col=None,
        run_id="runZZZ",
        settings={"run_type":"test"},
        dry_run=False,
        log_preview=False,
    )
    assert "account_name" in out.columns
    assert "account_id" in out.columns
    assert out["account_id"].isna().sum() == 2
    assert out.loc[0, "internal_row_id"] == "runZZZ-000000001"
    assert out.loc[1, "internal_row_id"] == "runZZZ-000000002"

def test_ingest_json_array(tmp_path):
    j = tmp_path / "a.json"
    j.write_text('{"records":[{"company":"Gamma","uuid":"u1"},{"company":"Delta","uuid":"u2"}]}')
    df = read_input_file(str(j), json_record_path="$.records[*]")
    out = apply_ingest_mapping(
        df,
        name_col="company",
        id_col="uuid",
        run_id="runAAA",
        settings={"run_type":"test"},
        dry_run=False,
        log_preview=False,
    )
    assert set(out.columns) >= {"account_name","account_id","internal_row_id"}
    assert out["account_id"].tolist() == ["u1","u2"]
