use datafusion::prelude::*;
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::Arc;
use prql_compiler::{compile, Options, Target};

pub struct PrqlExecutor {
    ctx: SessionContext,
}

impl PrqlExecutor {
    pub fn new() -> Self {
        Self {
            ctx: SessionContext::new(),
        }
    }

    pub async fn execute(&self, prql: &str, input: RecordBatch) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        // 1. Compile PRQL to SQL
        let options = Options {
            format: true,
            target: Target::Sql(None),
            signature_comment: false,
            color: false,
        };
        
        let sql = compile(prql, &options)?;

        // 2. Register the input record batch as a table named "input"
        self.ctx.register_batch("input", input)?;

        // 3. Execute the SQL
        let df = self.ctx.sql(&sql).await?;
        let results = df.collect().await?;

        // 4. Merge results into a single RecordBatch
        if results.is_empty() {
            return Err("query returned no results".into());
        }

        // For simplicity, we assume the result fits in memory and we take the first batch 
        // or concatenate them. DataFusion usually returns multiple batches.
        // In a real implementation, we should handle streaming.
        
        Ok(results[0].clone()) 
    }
}
