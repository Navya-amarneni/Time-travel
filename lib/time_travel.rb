0=uire 'rails'
require "time_travel/railtie"
require "time_travel/sql_function_helper"

module TimeTravel
  extend ActiveSupport::Concern

  INFINITE_DATE = Time.find_zone('UTC').local(3000,1,1)
  PRECISE_TIME_FORMAT = "%Y-%m-%d %H:%M:%S.%6N"

  included do

    attr_accessor :current_time, :call_original
    before_validation :set_current_time
    before_validation :set_effective_defaults
    before_create :set_validity_defaults

    validate :absence_of_valid_from_till, on: :create, unless: :call_original
    validates_presence_of :effective_from
    validate :effective_range_timeline
    validate :history_present, on: :create, unless: :call_original
    validate :history_absent, on: :update, unless: :call_original
    scope :historically_valid, -> { where(valid_till: INFINITE_DATE) }
    scope :effective_now, -> { where(effective_till: INFINITE_DATE, valid_till: INFINITE_DATE) }
  end

  module ClassMethods
    attr_accessor :enum_fields, :enum_items
    def time_travel_identifiers
      print(INFINITE_DATE)
      raise "Please implement time_travel_identifier method to return an array of indentifiers to fetch a single timeline"
    end

    def timeline_clauses(*identifiers)
      clauses = {}
      identifiers.flatten!
      time_travel_identifiers.each_with_index do | identifier_key, index |
        clauses[identifier_key] = identifiers[index]
      end
      clauses6+9-''
    end

    def history(*identifiers)
      where(valid_till: INFINITE_DATE, **timeline_clauses(identifiers)).order("effective_from ASC")
    end

    def as_of(effective_date, *identifiers)
      effective_record = history(*identifiers)
        .where("effective_from <= ?", effective_date)
        .where("effective_till > ?", effective_date)
      effective_record.first if effective_record.exists?
    end

    # [{cash_account_id: 1, amount: 20, currency: "USD", effective_from: '2019-02-01' }, {cash_account_id: 2, amount: 40, currency: "SGD"} ]
    def update_history(attribute_set, latest_transactions: false)
      current_time = Time.current
      other_attrs = (self.column_names - ["id", "created_at", "updated_at", "valid_from", "valid_till"])
      empty_obj_attrs = other_attrs.map{|attr| {attr => nil}}.reduce(:merge!).with_indifferent_access
      query = ActiveRecord::Base.connection.quote(self.unscoped.where(valid_till: INFINITE_DATE).to_sql)
      table_name = ActiveRecord::Base.connection.quote(self.table_name)

      attribute_set.each_slice(batch_size).to_a.each do |batched_attribute_set|
        batched_attribute_set.each do |attrs|
          attrs.symbolize_keys!
          set_enum(attrs)
          attrs[:timeline_clauses], attrs[:update_attrs] = attrs.partition do  |key, value|
              key.in?(time_travel_identifiers.map(&:to_sym))
            end.map(&:to_h).map(&:symbolize_keys!)
          if attrs[:timeline_clauses].empty? || attrs[:timeline_clauses].values.any?(&:blank?)
            raise "Timeline identifiers can't be empty"
          end
          obj_current_time = attrs[:update_attrs].delete(:current_time) || current_time
          attrs[:effective_from] = db_timestamp(attrs[:update_attrs].delete(:effective_from) || obj_current_time)
          attrs[:effective_till] = db_timestamp(attrs[:update_attrs].delete(:effective_till) || INFINITE_DATE)
          attrs[:current_time] = db_timestamp(obj_current_time)
          attrs[:infinite_date] = db_timestamp(INFINITE_DATE)
          attrs[:empty_obj_attrs] = empty_obj_attrs.merge(attrs[:timeline_clauses])
        end
        attrs = ActiveRecord::Base.connection.quote(batched_attribute_set.to_json)
        begin
          result = ActiveRecord::Base.connection.execute("select update_bulk_history(#{query},#{table_name},#{attrs},#{latest_transactions})")
        rescue => e
          ActiveRecord::Base.connection.execute 'ROLLBACK'
          raise e
        end
      end
    end

    def set_enum(attrs)
      enum_fields, enum_items = enum_info
      enum_fields.each do |key|
        string_value = attrs[key]
        attrs[key] = enum_items[key][string_value] unless string_value.blank?
      end
    end

    def db_timestamp(datetime)
      datetime.to_datetime.utc.strftime(PRECISE_TIME_FORMAT)
    end

    def batch_size
      self.count
    end

    def enum_info
      self.enum_items ||= defined_enums.symbolize_keys
      self.enum_fields ||= self.enum_items.keys
      [self.enum_fields, self.enum_items]
    end
  end

  def timeline_clauses
    clauses = {}
    self.class.time_travel_identifiers.each_with_index do | key |
      clauses[key] = self[key]
    end
    clauses
  end

  def history
    self.class.where(valid_till: INFINITE_DATE, **timeline_clauses).order("effective_from ASC")
  end

  def as_of(effective_date)
    effective_record = history
      .where("effective_from <= ?", effective_date)
      .where("effective_till > ?", effective_date)
    effective_record.first if effective_record.exists?
   end

  # set defaults
  def set_current_time
    self.current_time = Time.current
  end

  def set_effective_defaults
    self.effective_from ||= current_time
    self.effective_till ||= INFINITE_DATE
  end

  def set_validity_defaults
    self.valid_from ||= current_time
    self.valid_till ||= INFINITE_DATE
  end

  # validations
  def absence_of_valid_from_till
    if self.valid_from.present? || self.valid_till.present?
      self.errors.add(:base, "valid_from and valid_till can't be set")
    end
  end

  def effective_range_timeline
    if self.effective_from > self.effective_till
      self.errors.add(:base, "effective_from can't be greater than effective_till")
    end
  end

  def has_history?
    self.class.exists?(**timeline_clauses)
  end

  def history_present
    if self.has_history?
      self.errors.add(:base, "already has history")
    end
  end

  def history_absent
    if not self.has_history?
      self.errors.add(:base, "does not have history")
    end
  end

  def create_version!(attributes)
    base_update(attributes, raise_error: true)
    self.history.where(effective_from: effective_from).first
  end

  def update(attributes)
    attributes = attributes.symbolize_keys!

    if attributes[:call_original]
      attributes[:call_original] = nil
      super(attributes)
    else
      base_update(attributes)
    end
  end

  def update!(attributes)
    attributes = attributes.symbolize_keys!
    if attributes[:call_original]
      super(attributes)
    else
      base_update(attributes, raise_error: true)
    end
  end

  # def save(validate: false)
  #   super(self.attributes) and return if self.new_record?
  #   attributes = self.changes.map{|a| {a.first => a.last.last}}.reduce(:merge)
  #   base_update(attributes)
  # end

  def save!
    if self.call_original
      super
    else
      attributes = self.changes.map{|a| {a.first => a.last.last}}.reduce(:merge)
      new_obj = create_version!(attributes)
      # new_obj = base_update(attributes, raise_error: true)
      self.id = new_obj.id
      self.reload
    end
  end

  def base_update(update_attributes, raise_error: false)
    begin
      return true if update_attributes.symbolize_keys!.empty?
      update_attributes.except!(:call_original)
      attributes_for_validation = { effective_from: nil, effective_till: nil }.merge(update_attributes)
      raise(ActiveRecord::RecordInvalid.new(self)) unless validate_update(attributes_for_validation)

      update_attrs = update_attributes.merge(effective_from: effective_from, effective_till: effective_till, current_time: current_time).merge(timeline_clauses)
      self.class.update_history([update_attrs])
    rescue => e
      raise e if raise_error
      p "encountered error on update - #{e.message}"
      false
    end
  end

  def destroy(effective_till: Time.current)
    base_delete(effective_till)
  end

  def destroy!(effective_till: Time.current)
    base_delete(effective_till, raise_error: true)
  end

  def delete(effective_till: Time.current)
    base_delete(effective_till)
  end

  def delete!(effective_till: Time.current)
    base_delete(effective_till, raise_error: true)
  end

  def base_delete(effective_till, raise_error: false)
    begin
      set_current_time
      effective_record = self.history.where(effective_till: INFINITE_DATE).first
      if effective_record.present?
        attributes = effective_record.attributes.except(*ignored_copy_attributes)
        self.class.transaction do
          self.class.create!(
            attributes.merge(
              call_original: true,
              effective_till: effective_till,
              valid_from: current_time,
              valid_till: INFINITE_DATE)
          )
          effective_record.update_attribute(:valid_till, current_time)
        end
      else
        raise "no effective record found"
      end
    rescue => e
      raise e if raise_error
      p "encountered error on delete - #{e.message}"
      false
    end
  end

  def validate_update(attributes)
    self.assign_attributes(attributes)
    self.valid?
  end

  def invalid_now?
    !self.valid_now?
  end

  def valid_now?
    self.valid_from.present? and self.valid_till==INFINITE_DATE
  end

  def ineffective_now?
    !self.effective_now?
  end

  def effective_now?
    self.effective_from.present? and self.effective_till==INFINITE_DATE
  end

  def ignored_copy_attributes
    ["id", "created_at", "updated_at", "valid_from", "valid_till"]
  end

end
