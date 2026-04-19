package analyzer

import (
	"fmt"
	"strings"

	"github.com/jamesdrando/tucotuco/internal/parser"
	sqtypes "github.com/jamesdrando/tucotuco/internal/types"
)

func typeDescFromTypeName(node *parser.TypeName) (sqtypes.TypeDesc, error) {
	if node == nil {
		return sqtypes.TypeDesc{}, fmt.Errorf("%w: missing type name", sqtypes.ErrInvalidTypeDesc)
	}
	if node.Qualifier != nil {
		return sqtypes.TypeDesc{}, fmt.Errorf(
			"%w: qualified type name %q is not supported in Phase 1",
			sqtypes.ErrInvalidTypeDesc,
			qualifiedNameString(node.Qualifier),
		)
	}
	if len(node.Names) == 0 {
		return sqtypes.TypeDesc{}, fmt.Errorf("%w: missing type name", sqtypes.ErrInvalidTypeDesc)
	}

	parts := make([]string, 0, len(node.Names))
	for _, name := range node.Names {
		if name == nil || strings.TrimSpace(name.Name) == "" {
			return sqtypes.TypeDesc{}, fmt.Errorf("%w: missing type name", sqtypes.ErrInvalidTypeDesc)
		}
		parts = append(parts, name.Name)
	}

	text := canonicalTypeNameText(strings.Join(parts, " "))
	if len(node.Args) > 0 {
		args := make([]string, 0, len(node.Args))
		for _, arg := range node.Args {
			rendered, err := renderTypeArgument(arg)
			if err != nil {
				return sqtypes.TypeDesc{}, err
			}
			args = append(args, rendered)
		}
		text += "(" + strings.Join(args, ",") + ")"
	}

	return sqtypes.ParseTypeDesc(text)
}

func canonicalTypeNameText(text string) string {
	switch strings.ToUpper(strings.TrimSpace(text)) {
	case "CHARACTER":
		return "CHAR"
	case "CHARACTER VARYING":
		return "VARCHAR"
	default:
		return text
	}
}

func renderTypeArgument(node parser.Node) (string, error) {
	switch node := node.(type) {
	case *parser.IntegerLiteral:
		return node.Text, nil
	case *parser.FloatLiteral:
		return node.Text, nil
	case *parser.UnaryExpr:
		switch node.Operator {
		case "+", "-":
			value, err := renderTypeArgument(node.Operand)
			if err != nil {
				return "", err
			}
			return node.Operator + value, nil
		default:
			return "", fmt.Errorf("%w: unsupported unary operator %q in type argument", sqtypes.ErrInvalidTypeDesc, node.Operator)
		}
	default:
		return "", fmt.Errorf("%w: unsupported type argument %T", sqtypes.ErrInvalidTypeDesc, node)
	}
}
