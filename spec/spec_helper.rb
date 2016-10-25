require 'sequel'

$: << File.join(File.dirname(__FILE__), '..', 'lib')

Sequel::Database.extension :select_order_clauses

DB = Sequel.sqlite

DB.create_table :users do
  primary_key :id
  Time :created_at
  String :description
end

class User < Sequel::Model; end

require 'pry'
require 'minitest/autorun'
require 'minitest/rg'
