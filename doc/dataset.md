# データセット
TODO

## Glossary


### Location
 * S3
 * LocalFilesystem
 * InMemory


### Version
 * v1


### Identity
`{symbol}`_`{exchange}`_`{channel}`


### Channel
 * executionboard
 * ohlc1min
 
 ...

### Symbol
 * `FXBTCJPY`
 * `BTCJPY`
 * `FXBTCJPY=BTCJPY`
 
 
### Format
 * log
   * Format: log (each line is JSON)
   * Filtered: None 
   
 * loglzma
   * Format: log-LZMA
   * Filtered: None
 
 * tsvlzma
   * Format: TSV
   * Filtered: None
     
 * sqlite
   * Filtered: None
  
 * sqlite,reduced=newprices`{timeunit}`
   * Filtered: see `OpenCloseNewPricesExecutionReader`
 
 * sqlite,reduced=ohlc`{timeunit}`
   * Filtered: see `OHLCExecutionReader`
 