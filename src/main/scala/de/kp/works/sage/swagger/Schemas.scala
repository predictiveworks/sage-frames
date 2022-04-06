package de.kp.works.sage.swagger

/**
 * Copyright (c) 2019 - 2022 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

import com.google.gson.JsonObject
import org.apache.spark.sql.types._

import scala.collection.JavaConversions.{asScalaSet, iterableAsScalaIterable}
import scala.collection.mutable

object Schemas extends Swagger {

  private var definitions:Option[JsonObject] = None

  private val referenceSchemas = mutable.HashMap.empty[String, StructType]
  private val directSchemas = mutable.HashMap.empty[String, StructType]

  build()

  def getSchema(schemaName:String):Option[StructType] = {

    if (directSchemas.contains(schemaName))
      Some(directSchemas(schemaName))

    else if (referenceSchemas.contains(schemaName))
      Some(referenceSchemas(schemaName))

    else None

  }

  def build():Unit = {
    /*
     * STEP #1: Extract schema definitions from SAGE
     * swagger file
     */
    definitions = Some(swagger.get("definitions").getAsJsonObject)
    /*
     * STEP #2: Build part schemas that are referenced
     * by other schemas
     */
    buildReferences()
    /*
     * STEP #3: Move through all non-reference schemas
     * and build DataFrame compliant descriptions
     */
    val schemaNames = definitions.get.keySet()
    schemaNames.foreach(schemaName => {

      if (!referenceSchemas.contains(schemaName)) {
        val schema = definitions.get.get(schemaName).getAsJsonObject
        directSchemas += schemaName -> buildStructType(schema)

      }

    })

  }
  /**
   * A helper method to build those schemas that
   * are referenced by other schemas.
   *
   * Note, the order of schema names is important
   * for a proper build process.
   */
  private def buildReferences():Unit = {

    val references = Seq(
      "Base",
      "CoaGroupType",
      "LedgerAccountBalanceDetails",
      /*
       * `LedgerAccount` is a reference schema that
       * references itself `non_recoverable_ledger_account`.
       *
       * The current implementation ignores self-
       * referencing fields
       */
      "LedgerAccount",
      "JournalCodeType",
      "JournalCode",
      "JournalLine",
      "Link",
      "TransactionOrigin",
      "Transaction",
      "Address",
      "ContactPersonType",
      "ContactPerson",
      "BankAccountDetails",
      "ContactTaxTreatment",
      "ContactCisDeductionRate",
      "ContactCisSettings",
      "Contact",
      "TaxRatePercentage",
      "ComponentTaxRate",
      "TaxRate",
      "BankAccountContact",
      "BankAccountDetails",
      "BankAccountContact",
      "BankAccount",
      "BankReconciliationStatus",
      "SalesPrice",
      "Product",
      "Rate",
      "Service",
      "TaxBreakdown",
      "PurchaseInvoiceLineItem",
      "ArtefactTaxAnalysis",
      "ArtefactDetailedTaxAnalysisBreakdown",
      "ArtefactDetailedTaxAnalysis",
      "Generic",
      "AllocatedArtefact",
      "ContactAllocation",
      "AllocatedPaymentArtefact",
      "PaymentOnAccount",
      "ContactPayment",
      "PaymentAllocation",
      "PurchaseCreditNoteLineItem",
      "PurchaseCorrectiveInvoice",
      "SalesArtefactAddress",
      "EuSalesDescription",
      "SalesInvoiceLineItem",
      "SalesCreditNoteLineItem",
      "QuoteStatus",
      "SalesQuoteLineItem",
      "ProfitBreakdown",
      "RecurringSalesInvoice",
      "SalesInvoiceQuoteEstimate",
      "SalesCorrectiveInvoice",
      "BaseJournalLine",
      "OtherPaymentLineItem",
      "StockItem",
      "BusinessType",
      "TaxScheme",
      "InvoiceSettingsDocumentHeadings",
      "InvoiceSettingsLineItemTitles",
      "FooterDetails",
      "PrintContactDetails",
      "AddressRegion",
      "MigrationStatus",
      "GBBoxData",
      "IEBoxData",
      "DefaultMessages",
      "ProfitAnalysis",
      "BusinessActivityType",
      "LegalFormType",
      "DefaultLedgerAccounts",
      "PrintStatements"
    )

    references.foreach(schemaName => {

      val schema = definitions.get.get(schemaName).getAsJsonObject

      val schemaType = schema.get("type").getAsString
      assert(schemaType == "object")

      val schemaProps = schema.get("properties").getAsJsonObject
      val requiredFields =
        if (schema.has("required")) {
          schema.get("required").getAsJsonArray
            .map(e => e.getAsString).toSeq
        }
        else Seq.empty[String]

      val fieldNames = schemaProps.keySet()
      val fields = mutable.ArrayBuffer.empty[StructField]

      fieldNames.foreach(fieldName => {

        val field = schemaProps.get(fieldName).getAsJsonObject
        val nullable = !requiredFields.contains(fieldName)

        if (field.has("$ref")) {

          val refName = getRefName(field)
          if (referenceSchemas.contains(refName) && refName != schemaName)
            fields += StructField(fieldName, referenceSchemas(refName), nullable = nullable)

          else {
            println(s"[WARN] Field name `$fieldName` with a self-reference: $schemaName")

          }

        } else if (field.has("type")) {

          val fieldType = field.get("type").getAsString
          fieldType match {
            /*
             * BASIC FIELD TYPES
             */
            case "boolean" =>
              fields += StructField(fieldName, BooleanType, nullable = nullable)

            case "integer" =>
              fields += StructField(fieldName, IntegerType, nullable = nullable)

            case "number" =>

              val format = field.get("format").getAsString
              format match {

                case "double" =>
                  fields += StructField(fieldName, DoubleType, nullable = nullable)

                case _ =>
                  throw new Exception(s"$fieldName NUMBER: $format")

              }

            case "string" =>
              fields += StructField(fieldName, StringType, nullable = nullable)

            /*
             * COMPLEX FIELD TYPES
             */
            case "array" =>

              val items = field.get("items").getAsJsonObject
              if (items.has("$ref")) {

                val refName = getRefName(items)
                if (referenceSchemas.contains(refName))
                  fields += StructField(fieldName,
                    ArrayType(referenceSchemas(refName), containsNull = true), nullable = nullable)

                else
                  throw new Exception(s"Unknown reference detected: $refName")

              }
              else if (items.has("type")) {
                val itemType = items.get("type").getAsString
                itemType match {

                  case "string" =>
                    fields += StructField(fieldName,
                      ArrayType(StringType, containsNull = true), nullable = nullable)

                  case _ => throw new Exception(s"$fieldName ARRAY: $itemType")

                }

              }

            case _ => throw new Exception(fieldType)

          }
        }

      })

      referenceSchemas += schemaName -> StructType(fields.toArray)

    })

  }
  /*
   * TODO Building a unique database identifier can be necessary
   */
  private def buildStructType(schema:JsonObject):StructType = {

    val schemaType = schema.get("type").getAsString
    assert(schemaType == "object")

    val schemaProps = schema.get("properties").getAsJsonObject
    val requiredFields =
      if (schema.has("required")) {
        schema.get("required").getAsJsonArray
          .map(e => e.getAsString).toSeq
      }
      else Seq.empty[String]

    val fieldNames = schemaProps.keySet()
    val fields = mutable.ArrayBuffer.empty[StructField]

    fieldNames.foreach(fieldName => {

      val field = schemaProps.get(fieldName).getAsJsonObject
      val nullable = !requiredFields.contains(fieldName)

      try {

        if (field.has("$ref")) {
          val refName = getRefName(field)
          if (referenceSchemas.contains(refName))
            fields += StructField(fieldName,
              referenceSchemas(refName), nullable = nullable)

          else {
            throw new Exception(s"Unknown reference detected: $refName")

          }
        }
        else if (field.has("type")) {

          val fieldType = field.get("type").getAsString
          fieldType match {
            /*
             * BASIC FIELD TYPES
             */
            case "boolean" =>
              fields += StructField(fieldName, BooleanType, nullable = nullable)

            case "integer" =>
              fields += StructField(fieldName, IntegerType, nullable = nullable)

            case "number" =>

              val format = field.get("format").getAsString
              format match {

                case "double" =>
                  fields += StructField(fieldName, DoubleType, nullable = nullable)

                case _ =>
                  throw new Exception(s"$fieldName NUMBER: $format")

              }

            case "string" =>
              fields += StructField(fieldName, StringType, nullable = nullable)
            /*
             * COMPLEX FIELD TYPES
             */
            case "array" =>

              val items = field.get("items").getAsJsonObject
              if (items.has("$ref")) {

                val refName = getRefName(items)
                if (referenceSchemas.contains(refName))
                  fields += StructField(fieldName,
                    ArrayType(referenceSchemas(refName), containsNull = true), nullable = nullable)

                else {
                  throw new Exception(s"Unknown reference detected: $refName")

                }
              }
              else if (items.has("type")) {

                val itemType = items.get("type").getAsString
                itemType match {

                  case "string" =>
                    fields += StructField(fieldName,
                      ArrayType(StringType, containsNull = true), nullable = nullable)

                  case _ => throw new Exception(s"$fieldName ARRAY: $itemType")

                }

              }

            case "object" =>
              val objectType = field2StructType(field)
              fields += StructField(fieldName, objectType, nullable = nullable)

            case _ => throw new Exception(fieldType)
          }

        }

      } catch {
        case t:Throwable => throw t
      }

    })

    StructType(fields.toArray)

  }

  private def field2StructType(field:JsonObject):StructType = {

    val properties = field.get("properties").getAsJsonObject
    val requiredFields =
      if (field.has("required")) {
        field.get("required").getAsJsonArray
          .map(e => e.getAsString).toSeq
      }
      else Seq.empty[String]

    val fieldNames = properties.keySet()
    val fields = mutable.ArrayBuffer.empty[StructField]

    fieldNames.foreach(fieldName => {

      val field = properties.get(fieldName).getAsJsonObject
      val nullable = !requiredFields.contains(fieldName)

      if (field.has("$ref"))
        throw new Exception(s"Nested properties detected.")

      else if (field.has("type")) {

        val fieldType = field.get("type").getAsString
        fieldType match {
          /*
           * BASIC FIELD TYPES
           */
          case "boolean" =>
            fields += StructField(fieldName, BooleanType, nullable = nullable)

          case "integer" =>
            fields += StructField(fieldName, IntegerType, nullable = nullable)

          case "number" =>

            val format = field.get("format").getAsString
            format match {

              case "double" =>
                fields += StructField(fieldName, DoubleType, nullable = nullable)

              case _ =>
                throw new Exception(s"$fieldName NUMBER: $format")

            }

          case "string" =>
            fields += StructField(fieldName, StringType, nullable = nullable)

          /*
           * COMPLEX FIELD TYPES
           */
          case "array" =>

            val items = field.get("items").getAsJsonObject
            val itemType = items.get("type").getAsString
            itemType match {

              case "object" =>
                val objectType = field2StructType(items)
                fields += StructField(fieldName, objectType, nullable = nullable)

              case "string" =>
                fields += StructField(fieldName,
                  ArrayType(StringType, containsNull = true), nullable = nullable)

              case _ => throw new Exception(s"$fieldName ARRAY: $itemType")

            }

          case "object" =>
            val objectType = field2StructType(field)
            fields += StructField(fieldName, objectType, nullable = nullable)

          case _ =>
            throw new Exception(s"Nested properties detected: $fieldType")
        }
      }

    })

    StructType(fields.toArray)

  }

}
