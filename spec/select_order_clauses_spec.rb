require 'spec_helper'

class SelectOrderClausesSpec < Minitest::Spec
  it "should have a version" do
    assert Sequel::SelectOrderClauses::VERSION.is_a?(String)
  end

  describe "select_order" do
    def assert_select_order(ds:, selects:)
      initial = ds
      final = ds.select_order

      assert_equal selects, final.opts[:select]

      # Make sure that dataset is cached.
      assert_equal final.object_id, ds.select_order.object_id
    end

    it "should select only the order clauses" do
      assert_select_order(
        ds: User.dataset,
        selects: nil,
      )

      assert_select_order(
        ds: User.dataset.order_by(Sequel.desc(:id)),
        selects: [Sequel.as(:id, :order_0)],
      )

      assert_select_order(
        ds: User.dataset.order_by(Sequel.desc(:id), :other_column),
        selects: [Sequel.as(:id, :order_0), Sequel.as(:other_column, :order_1)],
      )
    end
  end

  describe "append_order_as_selection" do
    def assert_order_append(ds:, selects_to_append:, order_names:)
      initial = ds
      final = ds.append_order_as_selection

      assert_equal (initial.opts[:select] || []) + selects_to_append, final.opts[:select] || []

      order_info = final.opts.fetch(:order_info)

      assert_equal order_names, order_info.map { |h| h.fetch(:name) }

      # Make sure that dataset is cached.
      assert_equal final.object_id, ds.append_order_as_selection.object_id
    end

    it "should do nothing to queries without order_by clauses" do
      assert_equal User.dataset, User.dataset.append_order_as_selection
    end

    describe "for simple symbol columns" do
      it "when they are missing" do
        assert_order_append \
          ds: User.dataset.select(:id).order_by(:description),
          selects_to_append: [Sequel.as(:description, :order_0)],
          order_names: [:order_0]

        assert_order_append \
          ds: User.dataset.select(:id).order_by(:fake_column),
          selects_to_append: [Sequel.as(:fake_column, :order_0)],
          order_names: [:order_0]
      end

      it "when they are present" do
        assert_order_append \
          ds: User.dataset.select(:id).order_by(:id),
          selects_to_append: [],
          order_names: [:id]

        assert_order_append \
          ds: User.dataset.select(:created_at).order_by(:created_at),
          selects_to_append: [],
          order_names: [:created_at]
      end
    end

    describe "when the dataset has been made via #from_self" do
      it "when they are present" do
        assert_order_append \
          ds: User.dataset.select(:id).from_self.order_by(:id),
          selects_to_append: [],
          order_names: [:id]

        assert_order_append \
          ds: User.dataset.select(:users__created_at).from_self.order_by(:created_at),
          selects_to_append: [],
          order_names: [:created_at]
      end
    end

    describe "for qualified columns" do
      it "when they are missing" do
        assert_order_append \
          ds: User.dataset.select(:id).order_by(Sequel.qualify(:other_table, :fake_column)),
          selects_to_append: [Sequel.qualify(:other_table, :fake_column).as(:order_0)],
          order_names: [:order_0]

        assert_order_append \
          ds: User.dataset.select(:id).order_by(:other_table__fake_column),
          selects_to_append: [Sequel.qualify(:other_table, :fake_column).as(:order_0)],
          order_names: [:order_0]
      end

      it "when they are present" do
        assert_order_append \
          ds: User.dataset.select(Sequel.qualify(:users, :id)).order_by(:id),
          selects_to_append: [],
          order_names: [:id]

        assert_order_append \
          ds: User.dataset.select(Sequel.qualify(:users, Sequel::SQL::Identifier.new(:id))).order_by(:id),
          selects_to_append: [],
          order_names: [:id]

        assert_order_append \
          ds: User.dataset.select(:id).order_by(Sequel.qualify(:users, :id)),
          selects_to_append: [],
          order_names: [:id]

        assert_order_append \
          ds: User.dataset.select(:created_at).order_by(:users__created_at),
          selects_to_append: [],
          order_names: [:created_at]
      end
    end

    describe "for columns with aliases" do
      it "should recognize that ORDER BY can take either the column name or its alias" do
        assert_order_append \
          ds: User.dataset.select(Sequel.as(:description, :des)).order_by(:des),
          selects_to_append: [],
          order_names: [:des]

        assert_order_append \
          ds: User.dataset.select(Sequel.as(:description, :des)).order_by(:description),
          selects_to_append: [],
          order_names: [:des]
      end

      it "that are also qualified" do
        assert_order_append \
          ds: User.dataset.select(Sequel.qualify(:users, :description).as(:des)).order_by(:description),
          selects_to_append: [],
          order_names: [:des]

        assert_order_append \
          ds: User.dataset.select(Sequel.qualify(:users, :description).as(:des)).order_by(:des),
          selects_to_append: [],
          order_names: [:des]

        assert_order_append \
          ds: User.dataset.select(Sequel.as(:description, :des)).order_by(Sequel.qualify(:users, :description)),
          selects_to_append: [],
          order_names: [:des]

        assert_order_append \
          ds: User.dataset.select(:created_at___c_at).order_by(:users__created_at),
          selects_to_append: [],
          order_names: [:c_at]

        assert_order_append \
          ds: User.dataset.select(Sequel.qualify(:users, :description).as(:des)).order_by(:description),
          selects_to_append: [],
          order_names: [:des]
      end
    end

    describe "when ordering by arbitrary expressions" do
      it "when they are missing" do
        assert_order_append \
          ds: User.dataset.select(:id).order_by{function(:column1, 2)},
          selects_to_append: [Sequel.virtual_row{function(:column1, 2)}.as(:order_0)],
          order_names: [:order_0]

        assert_order_append \
          ds: User.dataset.select(:id).order_by{column - 2},
          selects_to_append: [Sequel.virtual_row{column - 2}.as(:order_0)],
          order_names: [:order_0]

        assert_order_append \
          ds: User.dataset.select(:id).order_by(Sequel.lit('function(column1, 2)')),
          selects_to_append: [Sequel.lit('function(column1, 2)').as(:order_0)],
          order_names: [:order_0]
      end

      it "when they are present" do
        assert_order_append \
          ds: User.dataset.select{function(:column1, 2).as(:my_column)}.order_by{function(:column1, 2)},
          selects_to_append: [],
          order_names: [:my_column]
      end

      it "when they are present but don't have a name we can safely predict" do
        assert_order_append \
          ds: User.dataset.select{function(:column1, 2)}.order_by{function(:column1, 2)},
          selects_to_append: [Sequel.virtual_row{function(:column1, 2)}.as(:order_0)],
          order_names: [:order_0]
      end
    end

    describe "when a selection includes a ColumnAll" do
      describe "and is not connected to a model" do
        it "when ordering by a column qualified to that table" do
          assert_order_append \
            ds: DB[:users].select_all(:users).order_by(Sequel.qualify(:users, :id)),
            selects_to_append: [],
            order_names: [:id]
        end

        it "when ordering by a column qualified to a different table" do
          assert_order_append \
            ds: DB[:users].select_all(:users).order_by(Sequel.qualify(:users_2, :id)),
            selects_to_append: [Sequel.qualify(:users_2, :id).as(:order_0)],
            order_names: [:order_0]
        end

        it "when ordering by an unqualified column" do
          assert_order_append \
            ds: DB[:users].select_all(:users).order_by(:id),
            selects_to_append: [Sequel.as(:id, :order_0)],
            order_names: [:order_0]
        end

        it "when ordering by an arbitrary expression" do
          assert_order_append \
            ds: DB[:users].select_all(:users).order_by{function(:column, 2)},
            selects_to_append: [Sequel.virtual_row{function(:column, 2)}.as(:order_0)],
            order_names: [:order_0]
        end
      end

      describe "and is connected to a model so we can look up columns" do
        it "when ordering by an unqualified column that is present in the table" do
          assert_order_append \
            ds: User.dataset.select_all(:users).order_by(:id),
            selects_to_append: [],
            order_names: [:id]

          assert_order_append \
            ds: User.dataset.select_all(:users).order_by(:description),
            selects_to_append: [],
            order_names: [:description]
        end

        it "when ordering by an unqualified column that is not present in the table" do
          assert_order_append \
            ds: User.dataset.select_all(:users).order_by(:fake_column),
            selects_to_append: [Sequel.as(:fake_column, :order_0)],
            order_names: [:order_0]
        end
      end
    end
  end
end
