# mysql-dds

Loadable MySQL functions for working with DDSKetches. DDSketches approximate a
dataset by grouping values in the underlying dataset into buckets with
exponentially increasing width and storing a count of occurrences for each
bucket. The accuracy of a DDSketch is determined by a the ⍺ paremeter, which
bounds the relative error of the quantile estimate. Setting a lower ⍺ yields
more accurate quantiles at the cost of an increased number of buckets.

Based on [this paper](https://arxiv.org/pdf/1908.10693.pdf).

## Binary Encoding

Sketches are stored in MySQL in binary columns with the following format.

* `version: unit8` - Version of the sketch. Exists so that we can safely modify the binary format if needed. Current version is `1`. 
* `gamma: float32` - `gamma = (1 + ⍺)/(1 - ⍺)`
* `sum: float32` - the sum of all measurements in the sketch (summed before bucketing). Stored so that an exact mean value can be calulated.
* `count: 1 - 10 byte unsigned varint` - the number of measurements in the sketch. This could also be calculated by summing the counts in the individual buckets, but this is stored separately so a mean could be calculated without needing to parse the buckets.
* `[buckets]: [bucket_key, bucket_value]` - repeating
    * `bucket key: 1 - 3 byte unsigned varint` - Determines the range of values represented by this bucket, centered around `(2 * metadata.gamma ^ bucket_key) / (gamma + 1)`. Bucket keys are delta encoded. The first bucket key will be a normal varint. Subsequent keys are expressed as the difference between the present bucket key and the previous bucket key. Example: bucket keys 10 and 15 would be serialized as 10 (absolute value) and 5 (10 + 5 = 15). This encoding scheme is used to minimize required storage space.
    * `bucket value: 1 - 10 byte unisgined varint` - The number of measurements in the sketch within the bounds indicated by the bucket key.

## Limitations

* Sketch observations cannot be negative. Representing negative measurements would require a separate set of buckets and latencies are presumed to always be positive.
* All observations less than 1 are rounded up to 1 to avoid needing an unbounded number of negative bucket indexes. The sketches don't internally specify units, so it's recommended to use a suitably small unit where rounding up to one won't cause issues (such as microseconds or nanoseconds).

## MySQL Functions

* `dds_sum(string: sketch) -> string: sketch` - Aggregate function that combines all of the input sketches into a single output sketch. Sketches can be combined without losing accuracy. All input sketches must have the same value for gamma. Merging sketches with different values for gamma will result in a all outputs being null after the first gamma difference is detected.
* `dds_quantile(real: quantile, string: sketch) -> real: value_at_quantile` - Returns the estimate of sketch measurements at the given quantile. Result is guaranteed to be ⍺-accurate (`abs(quantile_estimate - true_quantile) <= ⍺ * true_quantile`).
* `dds_merge(string: sketch_a, string: sketch_b) -> string: merged_sketch` - Combines `sketch_a` and `sketch_b` into a single sketch. Useful for updating a sketch row with new data (`update ... set sketch = dds_merge(sketch, $NEW_SKETCH)`).
* `dds_mean(string: sketch) -> real: mean` - Returns the mean value of a given sketch.
* `dds_count(string: sketch) -> real: count` - Returns the number of measurements in the given sketch.
* `dds_total(string: sketch) -> real: total` - Returns the total of all of the measurements in a given sketch.
* `dds_invalid(string: sketch) -> real: error` - Returns 1 if the input sketch is invalid, 0 otherwise.
* `dds_json(string: sketch) -> string: json` - Returns the sketch, including the buckets, as JSON.
* `dds_inspect(string: sketch) -> string: inspected` - Shows the sketch in a human readable format. You should probably use `dds_json` instead.
