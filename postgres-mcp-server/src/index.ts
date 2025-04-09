import {
	McpServer,
	ResourceTemplate,
} from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import pkg from "pg";
const { Pool } = pkg;
import dotenv from "dotenv";

dotenv.config();

// PostgreSQL connection pool
const pool = new Pool({
	connectionString: process.env.DATABASE_URL,
	ssl: process.env.DB_SSL === "true" ? { rejectUnauthorized: false } : false,
	max: 20,
});

// Create server instance
const server = new McpServer({
	name: "postgres-mcp-server",
	version: "1.0.0",
	capabilities: {
		resources: {},
		tools: {},
		prompts: {},
	},
});

interface TableRow {
	table_schema: string;
	table_name: string;
}

// Helper to get database tables
async function getTables() {
	const query = `
    SELECT 
      table_schema, 
      table_name
    FROM information_schema.tables 
    WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
    ORDER BY table_schema, table_name;
  `;
	const result = await pool.query(query);
	return result.rows as TableRow[];
}

// Helper to get table schema
async function getTableSchema(schema: string, table: string) {
	const query = `
    SELECT 
      column_name, 
      data_type, 
      is_nullable,
      column_default
    FROM information_schema.columns
    WHERE table_schema = $1 AND table_name = $2
    ORDER BY ordinal_position;
  `;
	const result = await pool.query(query, [schema, table]);
	return result.rows;
}

// Register resources for table schemas
server.resource(
	"table_schema",
	new ResourceTemplate("postgres://schema/{schema}/table/{table}", {
		list: undefined,
	}),
	{
		name: "Database Table Schema",
		description: "Get the schema for a specific database table",
	},
	async (uri, variables) => {
		try {
			const tableSchema = await getTableSchema(
				variables.schema as string,
				variables.table as string
			);
			return {
				contents: [
					{
						uri: uri.toString(),
						mimeType: "application/json",
						text: JSON.stringify(tableSchema, null, 2),
					},
				],
			};
		} catch (error) {
			console.error("Error fetching schema:", error);
			throw new Error(
				`Failed to fetch schema for ${variables.schema}.${variables.table}`
			);
		}
	}
);

// Register resource for listing all tables
server.resource(
	"database_tables",
	"postgres://tables" as string,
	{
		name: "All Database Tables",
		description: "List all tables in the database",
	},
	async (uri) => {
		try {
			const tables = await getTables();
			return {
				contents: [
					{
						uri: uri.toString(),
						mimeType: "application/json",
						text: JSON.stringify(tables, null, 2),
					},
				],
			};
		} catch (error) {
			console.error("Error listing tables:", error);
			throw new Error("Failed to list database tables");
		}
	}
);

// Register tool for executing read-only queries
server.tool(
	"execute_query",
	"Execute a read-only SQL query",
	{
		query: z.string().describe("SQL query to execute (SELECT only)"),
		params: z.array(z.any()).optional().describe("Query parameters"),
	},
	async ({ query, params = [] }) => {
		// Security check: ensure query is read-only
		const normalizedQuery = query.trim().toLowerCase();
		if (
			!normalizedQuery.startsWith("select") &&
			!normalizedQuery.startsWith("with")
		) {
			return {
				isError: true,
				content: [
					{
						type: "text",
						text: "Only SELECT queries are allowed for security reasons.",
					},
				],
			};
		}

		try {
			const result = await pool.query(query, params);

			return {
				content: [
					{
						type: "text",
						text: JSON.stringify(
							{
								rowCount: result.rowCount,
								rows: result.rows,
								fields: result.fields.map((f) => ({
									name: f.name,
									dataTypeID: f.dataTypeID,
								})),
							},
							null,
							2
						),
					},
				],
			};
		} catch (error: any) {
			console.error("Query execution error:", error);
			return {
				isError: true,
				content: [
					{
						type: "text",
						text: `Error executing query: ${error.message}`,
					},
				],
			};
		}
	}
);

// Register tool for table analysis
server.tool(
	"analyze_table",
	"Get summary statistics for a table",
	{
		schema: z.string().describe("Database schema"),
		table: z.string().describe("Table name"),
		columns: z
			.array(z.string())
			.optional()
			.describe("Specific columns to analyze"),
	},
	async ({ schema, table, columns = [] }) => {
		try {
			// Get table schema first to validate table exists
			const tableSchema = await getTableSchema(schema, table);

			if (tableSchema.length === 0) {
				return {
					isError: true,
					content: [
						{ type: "text", text: `Table ${schema}.${table} not found` },
					],
				};
			}

			// If no columns specified, analyze numeric columns
			const targetColumns =
				columns.length > 0
					? columns
					: tableSchema
							.filter((col) =>
								["int", "float", "numeric", "decimal"].some((t) =>
									col.data_type.includes(t)
								)
							)
							.map((col) => col.column_name);

			if (targetColumns.length === 0) {
				return {
					content: [
						{ type: "text", text: "No numeric columns found for analysis" },
					],
				};
			}

			// Create analysis query with stats for each column
			const statsSelects = targetColumns
				.map(
					(col) => `
        min(${col}) as "${col}_min", 
        max(${col}) as "${col}_max", 
        avg(${col}) as "${col}_avg", 
        percentile_cont(0.5) within group (order by ${col}) as "${col}_median",
        count(${col}) as "${col}_count",
        count(*) - count(${col}) as "${col}_nulls"
      `
				)
				.join(", ");

			const analysisQuery = `
        SELECT ${statsSelects}
        FROM "${schema}"."${table}"
      `;

			const result = await pool.query(analysisQuery);

			return {
				content: [
					{
						type: "text",
						text: JSON.stringify(result.rows[0], null, 2),
					},
				],
			};
		} catch (error: any) {
			console.error("Analysis error:", error);
			return {
				isError: true,
				content: [
					{
						type: "text",
						text: `Error analyzing table: ${error.message}`,
					},
				],
			};
		}
	}
);

// Register prompts for common data analysis tasks
server.prompt(
	"table_exploration",
	"Explore a database table",
	{
		schema: z.string().describe("Database schema"),
		table: z.string().describe("Table name"),
	},
	async (args) => {
		const { schema, table } = args;
		return {
			messages: [
				{
					role: "user",
					content: {
						type: "text",
						text: `I'd like to explore the ${schema}.${table} table in my database. First show me the schema, then run some basic analysis on the table, and finally suggest some useful queries that would give me insights about the data in this table.`,
					},
				},
			],
		};
	}
);

server.prompt(
	"data_quality_check",
	"Check data quality in a table",
	{
		schema: z.string().describe("Database schema"),
		table: z.string().describe("Table name"),
	},
	async (args) => {
		const { schema, table } = args;
		return {
			messages: [
				{
					role: "user",
					content: {
						type: "text",
						text: `Help me check the data quality in ${schema}.${table}. I'd like to know:
1. If there are any null values and what percentage of each column is null
2. The distribution of values in key columns
3. Any outliers or anomalies that might indicate data issues
4. Recommendations for improving data quality`,
					},
				},
			],
		};
	}
);

// Start the server
async function main() {
	try {
		// Test database connection
		const client = await pool.connect();
		console.error("Successfully connected to PostgreSQL");
		client.release();

		// Start MCP server
		const transport = new StdioServerTransport();
		await server.connect(transport);
		console.error("Postgres MCP Server running on stdio");
	} catch (error) {
		console.error("Failed to start server:", error);
		process.exit(1);
	}
}

main().catch((error) => {
	console.error("Fatal error:", error);
	process.exit(1);
});
