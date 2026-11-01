# Flux bookings pipeline

## Run

```bash
pip install -r requirements.txt
python -m src.pipeline                                          # process all batches in data/
python -m src.pipeline --batch bookings_batch2_2025-09-02.csv   # re-process one batch (safe)
python -m pytest -v                                                           # tests
```

To start from scratch, delete the `output/` folder.

