package redshift

import (
	"database/sql"
	"fmt"
	"github.com/hashicorp/errwrap"
	"github.com/hashicorp/terraform/helper/schema"
	"log"
	"time"
)

func redshiftExternalSchemaDataCatalog() *schema.Resource {
	return &schema.Resource{
		Create: resourceDataCatalogSchemaCreate,
		Read:   resourceDataCatalogSchemaRead,
		Update: resourceDataCatalogSchemaUpdate,
		Delete: resourceDataCatalogSchemaDelete,
		Exists: resourceDataCatalogSchemaExists,
		Importer: &schema.ResourceImporter{
			State: resourceDataCatalogSchemaImport,
		},

		Schema: map[string]*schema.Schema{
			"schema_name": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "The name of the external schema in Redshift",
			},
			"database_name": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The name of the database in the AWS Glue Data Catalog. Immutable after creation.",
			},
			"iam_role": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The ARN of the IAM role with S3 and AWS Glue Data Catalog access permissions. Immutable after creation.",
			},
			"owner": {
				Type:        schema.TypeInt,
				Optional:    true,
				Computed:    true,
				Description: "Defaults to user specified in provider",
			},
			"cascade_on_delete": {
				Type:        schema.TypeBool,
				Optional:    true,
				Description: "Keyword that indicates to automatically drop all objects in the schema, such as tables and functions. By default it doesn't for your safety",
				Default:     false,
			},
		},
	}
}

func resourceDataCatalogSchemaExists(d *schema.ResourceData, meta interface{}) (b bool, e error) {
	// Exists - This is called to verify a resource still exists. It is called prior to Read,
	// and lowers the burden of Read to be able to assume the resource exists.
	client := meta.(*Client).db

	var name string

	var existenceQuery = "SELECT nspname FROM pg_namespace WHERE oid = $1"

	log.Printf("Does external schema exist query: %s, %d", existenceQuery, d.Id())

	err := client.QueryRow(existenceQuery, d.Id()).Scan(&name)
	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, errwrap.Wrapf(fmt.Sprintf("Error reading external schema with oid %d: {{err}}", d.Id() ), err)		
	}
	return true, nil
}

func resourceDataCatalogSchemaCreate(d *schema.ResourceData, meta interface{}) error {

	redshiftClient := meta.(*Client).db

	var createStatement string = "CREATE EXTERNAL SCHEMA " + d.Get("schema_name").(string) + " FROM DATA CATALOG DATABASE '" + d.Get("database_name").(string) + "' IAM_ROLE '" + d.Get("iam_role").(string) + "'"

	log.Printf("Create external schema statement: %s", createStatement)

	if _, err := redshiftClient.Exec(createStatement); err != nil {
		return errwrap.Wrapf(fmt.Sprintf("Error creating external schema %s: {{err}}", d.Get("schema_name").(string)), err)
	}

	//The changes do not propagate instantly
	time.Sleep(5 * time.Second)

	//If owner was specified, apply it through ALTER SCHEMA
	if _, ok := d.GetOk("owner"); ok {
		if err := updateDataCatalogSchemaOwner(d, redshiftClient); err != nil {
			return err
		}
	}

	var oid string

	err := redshiftClient.QueryRow("SELECT oid FROM pg_namespace WHERE nspname = $1", d.Get("schema_name").(string)).Scan(&oid)

	if err != nil {
		return errwrap.Wrapf(fmt.Sprintf("Error reading oid for created external schema %s: {{err}}", d.Get("schema_name").(string)), err)
	}

	log.Printf("Created external schema with oid: %d", oid)

	d.SetId(oid)

	readErr := readDataCatalogSchema(d, redshiftClient)

	return readErr
}

func resourceDataCatalogSchemaRead(d *schema.ResourceData, meta interface{}) error {

	redshiftClient := meta.(*Client).db

	err := readDataCatalogSchema(d, redshiftClient)

	return err
}

func readDataCatalogSchema(d *schema.ResourceData, db *sql.DB) error {
	var (
		schemaName   string
		owner        int
		databaseName string
		iamRole      string
	)

	err := db.QueryRow("SELECT nspname, nspowner, databasename, json_extract_path_text(esoptions, 'IAM_ROLE') FROM pg_namespace JOIN svv_external_schemas ON pg_namespace.oid = esoid WHERE pg_namespace.oid = $1", d.Id()).Scan(&schemaName, &owner, &databaseName, &iamRole)

	if err != nil {
		return errwrap.Wrapf(fmt.Sprintf("Error reading external schema information for oid %d: {{err}}", d.Id()), err)
	}

	d.Set("schema_name", schemaName)
	d.Set("owner", owner)
	d.Set("database_name", databaseName)
	d.Set("iam_role", iamRole)

	return nil
}

type ExecQueryer interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}


func updateDataCatalogSchemaOwner(d *schema.ResourceData, q ExecQueryer) error {
	var username = GetUsersnamesForUsesysid(q, []interface{}{d.Get("owner").(int)})

	if _, err := q.Exec("ALTER SCHEMA " + d.Get("schema_name").(string) + " OWNER TO " + username[0]); err != nil {
		return errwrap.Wrapf(fmt.Sprintf("Error updating external schema %s owner to %s: {{err}}", d.Get("schema_name").(string), username[0]), err)
	}

	return nil
}

func resourceDataCatalogSchemaUpdate(d *schema.ResourceData, meta interface{}) error {

	redshiftClient := meta.(*Client).db
	tx, txErr := redshiftClient.Begin()
	if txErr != nil {
		panic(txErr)
	}

	if d.HasChange("schema_name") {

		oldName, newName := d.GetChange("schema_name")
		alterSchemaNameQuery := "ALTER SCHEMA " + oldName.(string) + " RENAME TO " + newName.(string)

		if _, err := tx.Exec(alterSchemaNameQuery); err != nil {
			return errwrap.Wrapf(fmt.Sprintf("Error renaming external schema %s to %s: {{err}}", oldName.(string), newName.(string)), err)
		}
	}

	if d.HasChange("owner") {
		if err := updateDataCatalogSchemaOwner(d, tx); err != nil {
			return err
		}
	}

	err := readDataCatalogSchema(d, redshiftClient)

	if err != nil {
		tx.Rollback()
		return errwrap.Wrapf("Error performing rollback: {{err}}", err)
	}

	tx.Commit()
	return nil
}

func resourceDataCatalogSchemaDelete(d *schema.ResourceData, meta interface{}) error {

	client := meta.(*Client).db

	dropSchemaQuery := "DROP SCHEMA " + d.Get("schema_name").(string)

	if v, ok := d.GetOk("cascade_on_delete"); ok && v.(bool) {
		dropSchemaQuery += " CASCADE "
	}

	_, err := client.Exec(dropSchemaQuery)

	if err != nil {
		return errwrap.Wrapf(fmt.Sprintf("Error dropping external schema %s: {{err}}", d.Get("schema_name").(string)), err)
	}

	return nil
}

func resourceDataCatalogSchemaImport(d *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {
	if err := resourceDataCatalogSchemaRead(d, meta); err != nil {
		return nil, err
	}
	return []*schema.ResourceData{d}, nil
}
