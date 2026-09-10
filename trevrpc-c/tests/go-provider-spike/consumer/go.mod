module trev.zip/llc/trevrpc/trevrpc-c/tests/go-provider-spike/consumer

go 1.26.0

require (
    trev.zip/llc/trevrpc/trevrpc-c v0.3.0
    trev.zip/llc/trevrpc/trevrpc-c/provider/msquic/v2 v2.6.0-trevrpc.1
)

replace trev.zip/llc/trevrpc/trevrpc-c => ../neutral
replace trev.zip/llc/trevrpc/trevrpc-c/provider/msquic/v2 => ../provider
