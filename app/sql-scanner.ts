export enum TokenType {
    Create = 'create',
    Table = 'table',
    Index = 'index',
    Select = 'select',
    From = 'from',
    Primary = 'primary',
    Key = 'key',
    On = 'on',
    Where = 'where',
    Identifier = 'identifier',
    Number = 'number',
    String = 'string',
    LeftParen = 'left_paren',
    RightParen = 'right_paren',
    Star = 'star',
    Comma = 'comma',
    Equal = 'equal',
    Eof = 'eof',
}

export class Token {
    constructor(
        public readonly type: TokenType,
        public readonly lexeme: string,
    ) { }
}

const keywordsMap: Record<string, TokenType> = {
    create: TokenType.Create,
    table: TokenType.Table,
    index: TokenType.Index,
    on: TokenType.On,
    select: TokenType.Select,
    from: TokenType.From,
    where: TokenType.Where,
    primary: TokenType.Primary,
    key: TokenType.Key,
}

export function scanSql(source: string): Token[] {
    const tokens: Token[] = [];

    let start = 0;
    let current = 0;

    while (!isAtEnd()) {
        // We are at the beginning of the next lexeme.
        start = current;
        scanToken();
    }

    addToken(TokenType.Eof);
    return tokens;

    function scanToken() {
        const c = advance();
        switch (c) {
            case '(':
                addToken(TokenType.LeftParen);
                break;
            case ')':
                addToken(TokenType.RightParen);
                break;
            case ',':
                addToken(TokenType.Comma);
                break;
            case '=':
                addToken(TokenType.Equal);
                break;
            case '*':
                addToken(TokenType.Star);
                break;
            case ' ':
            case '\n':
            case '\t':
            case '\r':
                break;
            default:
                if (isAlpha(c)) {
                    identifier();
                } else if (isNumeric(c)) {
                    number();
                } else if (c === "'") {
                    string(false);
                } else if (c === '"') {
                    string(true);
                } else {
                    console.error(`Unexpected character '${c}'`);
                }
        }
    }

    function identifier() {
        while (isAlpha(peek())) {
            advance();
        }

        const text = source.substring(start, current);
        const keywordToken = keywordsMap[text.toLowerCase()];

        if (keywordToken) {
            addToken(keywordToken);
        } else {
            addToken(TokenType.Identifier, text);
        }
    }

    function number() {
        while (isNumeric(peek())) {
            advance();
        }

        const text = source.substring(start, current);
        addToken(TokenType.Number, text);
    }

    function string(isDoubleQuote: boolean) {
        advance();
        while (!isAtEnd() && peek() !== (isDoubleQuote ? '"' : "'")) {
            advance();
        }

        if (isAtEnd()) {
            throw new Error('Unterminated string');
        }

        advance();

        const text = source.substring(start + 1, current - 1);
        addToken(TokenType.String, text);
    }

    function isAlpha(c: string) {
        const code = c.charCodeAt(0);
        return (code >= 'a'.charCodeAt(0) && code <= 'z'.charCodeAt(0))
            || (code >= 'A'.charCodeAt(0) && code <= 'Z'.charCodeAt(0))
            || c === '_';
    }

    function isNumeric(c: string) {
        const code = c.charCodeAt(0);
        return code >= '0'.charCodeAt(0) && code <= '9'.charCodeAt(0);
    }

    function peek() {
        if (isAtEnd()) {
            return '\0';
        }
        return source.charAt(current);
    }

    function advance() {
        return source.charAt(current++);
    }

    function addToken(type: TokenType, lexeme?: string) {
        tokens.push(new Token(type, lexeme || ""));
    }

    function isAtEnd() {
        return current >= source.length;
    }
}