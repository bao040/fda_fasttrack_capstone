"""Basic pytest coverage for the fetch/transform logic (capstone requirement M7).
 
Loaded via importlib (not a normal package import) since jobs/fetch/ isn't
set up as an installable package — this works regardless of where pytest
is invoked from.
"""
import importlib.util
from pathlib import Path
 
MODULE_PATH = Path(__file__).resolve().parents[1] / "jobs" / "fetch" / "fetch_drug_ndc.py"
spec = importlib.util.spec_from_file_location("fetch_drug_ndc", MODULE_PATH)
fetch_drug_ndc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fetch_drug_ndc)
 
 
def test_sample_step_100_percent_keeps_every_record():
    assert fetch_drug_ndc._sample_step(100) == 1
 
 
def test_sample_step_50_percent_keeps_every_other_record():
    assert fetch_drug_ndc._sample_step(50) == 2
 
 
def test_sample_step_10_percent_keeps_one_in_ten():
    assert fetch_drug_ndc._sample_step(10) == 10
 
 
def test_sample_step_above_100_clamps_to_100():
    assert fetch_drug_ndc._sample_step(500) == fetch_drug_ndc._sample_step(100)
 
 
def test_sample_step_zero_or_negative_clamps_to_minimum_percent():
    assert fetch_drug_ndc._sample_step(0) == fetch_drug_ndc._sample_step(-50)