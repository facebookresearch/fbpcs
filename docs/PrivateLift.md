# Private Lift Games Pseudocode
## Sample of Facebook Input
Facebook inputs the opportunity data of a Lift study. This is not shared with the advertiser. We assume the `id_` column is aligned across publisher (facebook) and advertiser using Private ID protocol that runs before MPC game.
* Each row represents a person.
* The `id_` column can be in a different format based on the type of person id used by the participating advertiser.
* `opportunity` column is optional.
* `test_flag` indicates whether the person is in the test group or control group of the Lift study.
* `opportunity_timestamp` represents the unixtime when the person was served an opportunity.

```
id_,opportunity,test_flag,opportunity_timestamp
0,0,0,0
1,1,0,1605360340
2,0,0,0
3,1,0,1605360341
4,1,1,1605360337
5,1,0,160536033538
```

## Sample of Advertiser Input
The advertiser inputs the conversion data of a Lift study. This is not shared with Facebook.
* Because ids should be matched by PID process before play the Lift games, we expect a one-to-one mapping between the ids in this input and the ids in Facebook's input.
* Both "event_timestamps" and "values" are capped and padded to have 4 values. This means currently we support up to **4 conversions per user**.

```
id_,event_timestamps,values,feature_foo,feature_bar
0,[0,0,0,1605360335],[0,0,0,33],aaa,111
1,0,0,0,0
2,0,0,0,0
3,[0,0,1605564241,1606014035],[0,0,32,59],bbb,222
4,0,0,0,0
5,0,0,0,0
```

## Conversion Lift Game

### Inputs and Outputs
* Facebook Input: List of id_, opportunity (optional), test_flag, opportunity_timestamp
* Advertiser Input: List of id_, event_timestamps, values, any features (optional)
* Facebook Output: Xor share of Output statistics
* Advertiser Output: Xor share of Output statistics

### Output statistics
    testPopulation, //# of all the people in the test group
    controlPopulation, //# of all the people in the control group
    testConversions, //# of valid conversion made by the test population
    controlConversions, //# of valid conversion made by the control population
    testValue, //sum of the value of valid conversions in the test group
    controlValue, //sum of value of valid conversions in the control group
    testSquared, //user-grain sum of the squares of the values of valid conversions in the test group
    controlSquared, //user-grain sum of the squares of the values of valid conversions in the control group

### Facebook and Advertiser jointly compute in 2 PC

```
for each row in lists:
    if (opportunity (if present) and test_flag)
        testPopulation++
        for each event_timestamp and value:
            if (opportunity_timestamp < event_timestamp + 10)
                testConversions++
                testValue += value
                testValueSquared += value * value
    if (opportunity (if present) and not test_flag)
        controlPopulation++
        for each event_timestamp and value:
            if (opportunity_timestamp < event_timestamp + 10)
                controlConversions++
                controlValue += value
                controlValueSquared += value * value
  ```

We run the computation for the overall dataset and then again for each "cohort."
Each "cohort" is defined as a unique combination of all features provided by the
partner.
Output XOR share of the Output Statistics to each party.


## Aggregator Game

### Inputs and outputs
* Facebook Input:
```
    X shards of XOR share of output metrics from previous step
        testPopulation,
        controlPopulation,
        testConversions,
        controlConversions,
        testValue,
        controlValue,
        testSquared,
        controlSquared
    with X >= 1
```
* Advertiser Input: Same format as Facebook Input
* Facebook Output: Output metrics with all zeroes
* Advertiser Output: Aggregated output metrics

### Facebook and Advertiser jointly compute in 2 PC
```
for each shard S in all shards:
   for each metric M in S:
        aggregated.M += Xor M.value
```

Output aggregated Output Statistics to both parties or Advertiser only if they request so.
