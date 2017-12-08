# frozen_string_literal: true

require 'sequel'
require 'sequel/extensions/select_order_clauses/version'

module Sequel
  module SelectOrderClauses
    def select_order
      return self unless order = @opts[:order]

      cached_dataset(:_select_order_ds) do
        select(
          *order.map.with_index { |o, index|
            Sequel.as(
              normalize_expression(unwrap_order_expression(o)),
              "order_#{index}".to_sym,
            )
          }
        )
      end
    end

    def append_order_as_selection
      return self unless order = @opts[:order]
      return self if @opts[:order_info]

      cached_dataset(:_append_order_as_selection_ds) do
        # Note that since we're iterating over the order to return a modified
        # version of this dataset, we can't modify the order in this method and
        # remain sensible.
        ds = self

        selections = extract_selections(ds).map { |s| normalize_selection(s) }

        order_info =
          order.map.with_index do |o, index|
            exp = normalize_expression(unwrap_order_expression(o))
            dir = extract_direction(o)

            # Try to figure out which of the select expressions is going to
            # correspond to this order expression. This heuristic may not be
            # perfect, but do our best and raise an error if we find more than one
            # selection.
            expression_selects =
              selections.select do |s|
                selection_satisfies_expression?(s, exp)
              end

            name =
              case expression_selects.length
              when 0 then nil
              when 1
                expression_select = expression_selects.first

                # Once we have the SELECT expression that matches our ORDER BY
                # expression, we just extract its name so that we'll be able to
                # figure out how we sorted records later on. The exception is if
                # the matching SELECT expression is "table".* - in that case
                # we'll need to get the name from the ORDER BY expression.
                target_expression =
                  if expression_select.is_a?(Sequel::SQL::ColumnAll)
                    exp
                  else
                    expression_select
                  end

                extract_expression_name(target_expression)
              else
                raise "Found more than one selection in #{inspect} that matched the expression #{exp.inspect}: #{expression_selects.inspect}"
              end

            # After all that, we still might not have been able to get a name.
            # In that case, just append the ORDER BY expression to the SELECT
            # clause with a special alias that we'll use later.
            unless name
              name = "order_#{index}".to_sym
              ds = ds.select_append(Sequel.as(exp, name))
            end

            {name: name, direction: dir}.freeze
          end

        ds.clone(order_info: order_info.freeze)
      end
    end

    private

    # Our inputs should have already been simplified and normalized to the
    # extent possible, now the nitty-gritty of defining whatever equality
    # logic we really need to for them.

    # The question at hand: can we safely assume that this expression in the
    # SELECT clause will always represent this expression in the ORDER BY?
    def selection_satisfies_expression?(s, e)
      case s
      when Sequel::SQL::AliasedExpression
        # Order expressions can be simple references to aliases, which is a
        # pretty simple and useful case to check for. In other words:
        # `SELECT function() AS my_value FROM table ORDER BY my_value`
        if e.is_a?(Symbol) && s.alias == e
          true
        else
          # Otherwise, see whether the expression being aliased does what we
          # want. For example:
          # `SELECT function() AS my_value FROM table ORDER BY function()`
          selection_satisfies_expression?(normalize_selection(s.expression), e)
        end
      when Sequel::SQL::QualifiedIdentifier
        # SELECT "table"."column"
        case e
        when Symbol
          # SELECT "table"."column" FROM "table" ORDER BY "column"
          e = e.to_s if s.column.is_a?(String)
          s.column == e
        when Sequel::SQL::QualifiedIdentifier
          # SELECT "table"."column" FROM "table" ORDER BY "table"."column"
          s == e
        end
      when Sequel::SQL::ColumnAll
        # SELECT "table".*
        case e
        when Sequel::SQL::QualifiedIdentifier
          # Satisfies any column anchored on that table...
          s.table == e.table
        when Symbol
          # ...or a plain column reference that we can verify lives on this
          # model. Note that m.columns.include?(e) would leave out
          # lazy-loaded columns.
          respond_to?(:model) &&
            (m = model) &&
            m.table_name == s.table &&
            m.db_schema.has_key?(e)
        end
      else
        # These values could be anything - functions, mathematical operations,
        # literal SQL strings, etc. Just try for simple equality.
        s == e
      end
    end

    # In addition to the common normalization logic in normalize_expression(),
    # which can be applied to expressions in either the SELECT or ORDER BY
    # clauses, this method encapsulates an assumption that is safe to make
    # about expressions in SELECT but not in ORDER BY, that a simple
    # identifier (a symbol) on a single-source dataset must refer to a column
    # in the table. In an ORDER BY clause it could also refer to an alias in
    # the SELECT clause.
    def normalize_selection(s)
      s = normalize_expression(s)

      case s
      when Symbol
        if joined_dataset?
          s # Can't make any assumptions about the source table.
        else
          Sequel.qualify(first_source, s)
        end
      else
        s
      end
    end

    # Move more esoteric Sequel types to the baseline of symbols representing
    # identifiers and QualifiedIdentifiers representing table-column pairs, so
    # that it's easier for us to do comparisons without needing to define
    # equality logic between every combination of classes in the Sequel AST.
    def normalize_expression(expression)
      case expression
      when Symbol
        # Take care of symbol notations like :table__column___alias.
        table, column, aliaz =
          Sequel.split_symbol(expression).map { |part| part&.to_sym }

        exp = table ? Sequel.qualify(table, column) : column
        exp = Sequel.as(exp, aliaz) if aliaz
        exp
      when Sequel::SQL::Identifier
        # Identifier objects have their uses, but not here, where a symbol is
        # just fine.
        expression.value.to_sym
      when Sequel::SQL::QualifiedIdentifier
        t = expression.table
        c = expression.column

        if t.is_a?(Symbol) && c.is_a?(Symbol)
          expression
        else
          Sequel::SQL::QualifiedIdentifier.new(
            normalize_expression(t),
            normalize_expression(c),
          )
        end
      else
        # Other arbitrary expressions can just be passed through.
        expression
      end
    end

    def extract_selections(ds)
      if selections = ds.opts[:select]
        return selections
      end

      if (froms = ds.opts[:from]) && froms.length == 1
        from = unwrap_alias(froms.first)
        if from.is_a?(Sequel::Dataset)
          extract_selections(from)
        end
      end
    end

    def extract_expression_name(expression)
      case expression
      when Symbol
        expression
      when Sequel::SQL::AliasedExpression
        expression.alias
      when Sequel::SQL::Identifier
        extract_expression_name(expression.value)
      when Sequel::SQL::QualifiedIdentifier
        v = expression.column
        v = v.to_sym if v.is_a?(String)
        extract_expression_name(v)
      end
    end

    def unwrap_order_expression(order)
      case order
      when Sequel::SQL::OrderedExpression
        order.expression
      else
        order
      end
    end

    def unwrap_alias(expression)
      case expression
      when Sequel::SQL::AliasedExpression
        expression.expression
      else
        expression
      end
    end

    def extract_direction(order)
      case order
      when Sequel::SQL::OrderedExpression
        order.descending ? :desc : :asc
      else
        :asc
      end
    end
  end

  Dataset.register_extension(:select_order_clauses, SelectOrderClauses)
end
