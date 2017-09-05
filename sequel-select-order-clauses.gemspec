# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'sequel/extensions/select_order_clauses/version'

Gem::Specification.new do |spec|
  spec.name          = 'sequel-select-order-clauses'
  spec.version       = Sequel::SelectOrderClauses::VERSION
  spec.authors       = ["Chris Hanks"]
  spec.email         = ['christopher.m.hanks@gmail.com']
  spec.summary       = %q{Select the order clauses in a dataset}
  spec.description   = %q{Simple support for selecting the order clauses in a dataset}
  spec.homepage      = 'https://github.com/chanks/sequel-select-order-clauses'
  spec.license       = 'MIT'

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ['lib']

  spec.add_development_dependency 'bundler', '~> 1.6'
  spec.add_development_dependency 'rake'
  spec.add_development_dependency 'sqlite3'

  spec.add_dependency 'sequel', '~> 5.0'
end
