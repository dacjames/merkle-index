# Time Series Index

## Data Model

- Metric
- Index
- Series

### Metric

```js
{
    timestamp: Signed.Int(64)
    value: Int | Float | String
    tags: Set(Key, Index, Value)
}
```

### Series

```js
{
    start: Signed.Int(64)
    end: Signed.Int(64)
    values: Array(Signed.Int(32), Int | Float | String)
}
```







