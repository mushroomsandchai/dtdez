version: 2

macros:
  - name: payment_description
    description: A macro to convert a dictionary value to a table
    arguments:
      - name: payment_type
        description: payment_type as per nytaxi dataset
      - name: payment_description
        description: associated payment description

  - name: vendor_name
    description: A macro to convert a dictionary value to a table
    arguments:
      - name: vendor_id
        description: vendor id as per nytaxi dataset
      - name: vendor_name
        description: associated vendor name