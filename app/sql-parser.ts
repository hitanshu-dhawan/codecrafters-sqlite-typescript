import type { ColumnDefinition, DataValueType, TableSchema } from "./database-types";
import { Token, TokenType } from "./sql-scanner";

export enum NodeType {
    Select = 'select',
    CreateTable = 'create_table',
    CreateIndex = 'create_index',
}

export class WhereExpressionNode {
    constructor(
        public readonly columnName: string,
        public readonly value: DataValueType,
    ) { }
}

export class SelectNode {
    public readonly type = NodeType.Select;

    constructor(
        public readonly table: string,
        public readonly columns: string[],
        public readonly where: WhereExpressionNode | null,
    ) { }
}

export class CreateTableNode {
    public readonly type = NodeType.CreateTable;

    constructor(
        public readonly tableName: string,
        public readonly tableSchema: TableSchema,
    ) { }
}

export class CreateIndexNode {
    public readonly type = NodeType.CreateIndex;

    constructor(
        public readonly indexName: string,
        public readonly tableName: string,
        public readonly columns: string[],
    ) { }
}

export type SqlNode = SelectNode | CreateTableNode | CreateIndexNode;

export function parseSql(tokens: Token[]): SqlNode {
    let current = 0;

    return command();

    function command(): SqlNode {
        if (check(TokenType.Select)) {
            return select();
        }
        if (check(TokenType.Create)) {
            return create();
        }
        throw new Error(`Unsupported command`);
    }

    function select(): SelectNode {
        consume(TokenType.Select);
        const columns: string[] = [];

        while (!isAtEnd()) {
            const column = selectColumn();
            columns.push(column);
            if (!match(TokenType.Comma)) {
                break;
            }
        }
        consume(TokenType.From);
        consume(TokenType.Identifier);
        const tableName = previous().lexeme;

        if (match(TokenType.Where)) {
            const whereExpression = where();
            return new SelectNode(tableName, columns, whereExpression);
        }
        return new SelectNode(tableName, columns, null);
    }

    function selectColumn(): string {
        if (match(TokenType.Identifier)) {
            const columnIdentifier = previous();

            if (columnIdentifier.lexeme.toLowerCase() === "count" && match(TokenType.LeftParen)) {
                consume(TokenType.Star);
                consume(TokenType.RightParen);
                return 'count(*)';
            }

            return columnIdentifier.lexeme;
        } else if (match(TokenType.String)) {
            return previous().lexeme;
        }
        throw new Error("Column name was expected in SELECT");
    }

    function where(): WhereExpressionNode {
        const column = entityName("Column name in the WHERE condition was expected");

        consume(TokenType.Equal);

        if (match(TokenType.Number)) {
            const numberText = previous().lexeme;
            return new WhereExpressionNode(column, parseInt(numberText));
        } else if (match(TokenType.String)) {
            const string = previous().lexeme;
            return new WhereExpressionNode(column, string);
        } else {
            throw new Error('Wrong WHERE condition');
        }
    }

    function create(): SqlNode {
        consume(TokenType.Create);
        if (match(TokenType.Table)) {
            const tableName = entityName("Table name was expected");
            consume(TokenType.LeftParen);

            const columns: ColumnDefinition[] = [];

            while (!isAtEnd()) {
                const column = columnDef();
                columns.push(column);
                if (!match(TokenType.Comma)) {
                    consume(TokenType.RightParen);
                    break;
                }
            }

            return new CreateTableNode(tableName, columns);
        } else if (match(TokenType.Index)) {
            const indexName = entityName("Index name was expected");
            consume(TokenType.On);

            const tableName = entityName("Table name was expected");
            consume(TokenType.LeftParen);

            const columns = [];

            while (!isAtEnd()) {
                const column = entityName("Column name is expected in index definition");
                columns.push(column);
                if (!match(TokenType.Comma)) {
                    break;
                }
            }

            consume(TokenType.RightParen);

            return new CreateIndexNode(indexName, tableName, columns);
        }

        throw new Error(`Unsupported command`);
    }

    function columnDef(): ColumnDefinition {
        const name = entityName("Column name was expected");
        let isPrimaryKey = false;

        while (!isAtEnd()) {
            if (check(TokenType.Comma) || check(TokenType.RightParen)) {
                break;
            }
            if (check(TokenType.Primary) && checkNext(TokenType.Key)) {
                isPrimaryKey = true;
            }
            advance();
        }

        return { name, isPrimaryKey };
    }

    function entityName(error: string): string {
        if (!match(TokenType.Identifier, TokenType.String)) {
            throw new Error(error);
        }

        return previous().lexeme;
    }

    function consume(type: TokenType) {
        if (check(type)) {
            return advance();
        }
        throw new Error(`Unexpected token '${peek().lexeme}' of type '${peek().type}' where '${type}' was expected`)
    }

    function match(...types: TokenType[]) {
        for (const type of types) {
            if (check(type)) {
                advance();
                return true;
            }
        }
        return false;
    }

    function check(type: TokenType) {
        if (isAtEnd()) {
            return false;
        }
        return peek().type === type;
    }

    function checkNext(type: TokenType) {
        if (isAtEnd()) {
            return false;
        }
        return peekNext().type === type;
    }

    function advance() {
        if (!isAtEnd()) {
            current++;
        }
        return previous();
    }

    function isAtEnd() {
        return peek().type === TokenType.Eof;
    }

    function previous() {
        return tokens[current - 1];
    }

    function peek() {
        return tokens[current];
    }

    function peekNext() {
        return tokens[current + 1] || TokenType.Eof;
    }
}