<?php

declare(strict_types=1);

namespace bizley\migration\table;

use function call_user_func;
use Closure;
use function is_string;
use yii\base\BaseObject;
use yii\base\InvalidArgumentException;
use yii\db\cubrid\Schema as CubridSchema;
use yii\db\mssql\Schema as MssqlSchema;
use yii\db\mysql\Schema as MysqlSchema;
use yii\db\oci\Schema as OciSchema;
use yii\db\pgsql\Schema as PgsqlSchema;
use yii\db\sqlite\Schema as SqliteSchema;
use function in_array;
use function mb_strlen;
use function sprintf;
use function strpos;
use function substr;

/**
 * Class TableStructure
 * @package bizley\migration\table
 *
 * @property string $schema
 */
class TableStructure extends BaseObject
{
    public const SCHEMA_MSSQL = 'mssql';
    public const SCHEMA_OCI = 'oci';
    public const SCHEMA_PGSQL = 'pgsql';
    public const SCHEMA_SQLITE = 'sqlite';
    public const SCHEMA_CUBRID = 'cubrid';
    public const SCHEMA_MYSQL = 'mysql';
    public const SCHEMA_UNSUPPORTED = 'unsupported';

    /**
     * @var string
     */
    public $name;

    /**
     * @var TablePrimaryKey
     */
    public $primaryKey;

    /**
     * @var TableColumn[]
     */
    public $columns = [];

    /**
     * @var TableIndex[]
     */
    public $indexes = [];

    /**
     * @var TableForeignKey[]
     */
    public $foreignKeys = [];

    /**
     * @var bool
     */
    public $generalSchema = true;

    /**
     * @var bool
     */
    public $usePrefix = true;

    /**
     * @var string
     */
    public $dbPrefix;

    /**
     * @var string|null
     * @since 3.0.4
     * TODO: remove
     */
    public $tableOptionsInit;

    /**
     * @var Closure|string|null
     * Closure signature must be
     * function($table): string
     * where $table is reference to current TableStructure object.
     * @since 4.0.0
     */
    public $prepend;

    /**
     * @var Closure|string
     * Closure signature must be
     * function($table): string
     * where $table is reference to current TableStructure object.
     * @since 4.0.0
     */
    public $append;

    /**
     * @var Closure|string|null
     * Closure signature must be
     * function($table): string
     * where $table is reference to current TableStructure object.
     * @since 3.0.4
     */
    public $tableOptions;


    protected $_schema;

    /**
     * Returns schema type.
     * @return string
     */
    public function getSchema(): string
    {
        return $this->_schema;
    }

    /**
     * Returns schema code based on its class name.
     * @param null|string $schemaClass
     * @return string
     * @since 3.1
     */
    public static function identifySchema(?string $schemaClass): string
    {
        switch ($schemaClass) {
            case MssqlSchema::class:
                return self::SCHEMA_MSSQL;

            case OciSchema::class:
                return self::SCHEMA_OCI;

            case PgsqlSchema::class:
                return self::SCHEMA_PGSQL;

            case SqliteSchema::class:
                return self::SCHEMA_SQLITE;

            case CubridSchema::class:
                return self::SCHEMA_CUBRID;

            case MysqlSchema::class:
                return self::SCHEMA_MYSQL;

            default:
                return self::SCHEMA_UNSUPPORTED;
        }
    }

    /**
     * Sets schema type based on the currently used schema class.
     * @param string|null $schemaClass
     */
    public function setSchema(?string $schemaClass): void
    {
        $this->_schema = static::identifySchema($schemaClass);
    }

    /**
     * Renders table name.
     * @return string
     */
    public function renderName(): string
    {
        $tableName = $this->name;

        if (!$this->usePrefix) {
            return $tableName;
        }

        if ($this->dbPrefix && strpos($this->name, $this->dbPrefix) === 0) {
            $tableName = substr($this->name, mb_strlen($this->dbPrefix, 'UTF-8'));
        }

        return '{{%' . $tableName . '}}';
    }

    /**
     * Renders the migration structure.
     * @param array $mutedDependencies
     * @return string
     */
    public function render(array $mutedDependencies = []): string
    {
        return $this->renderTable()
            . $this->renderPk()
            . $this->renderIndexes()
            . $this->renderForeignKeys([], $mutedDependencies) . "\n";
    }

    /**
     * Renders the table.
     * @return string
     */
    public function renderTable(): string
    {
        $output = '';

        $prepend = null;
        if ($this->prepend !== null) {
            if (is_string($this->prepend)) {
                $prepend = $this->prepend;
            } elseif ($this->prepend instanceof Closure) {
                $prepend = call_user_func($this->prepend, $this);
            }
        }

        if ($prepend !== null) {
            $output .= $prepend;
        }

        $output .= sprintf('        $this->createTable(\'%s\', [', $this->renderName());

        foreach ($this->columns as $column) {
            $output .= "\n" . $column->render($this);
        }

        $tableOptions = null;
        if ($this->tableOptions !== null) {
            if (is_string($this->tableOptions)) {
                $tableOptions = $this->tableOptions;
            } elseif ($this->tableOptions instanceof Closure) {
                $tableOptions = call_user_func($this->tableOptions, $this);
            }
        }

        $output .= "\n" . sprintf(
            '        ]%s);',
            $tableOptions !== null ? ", {$tableOptions}" : ''
        ) . "\n";

        $append = null;
        if ($this->append !== null) {
            if (is_string($this->append)) {
                $append = $this->append;
            } elseif ($this->append instanceof Closure) {
                $append = call_user_func($this->append, $this);
            }
        }

        if ($append !== null) {
            $output .= $append;
        }

        return $output;
    }

    /**
     * Renders the primary key.
     * @return string
     */
    public function renderPk(): string
    {
        $output = '';

        if ($this->primaryKey->isComposite()) {
            $output .= "\n" . $this->primaryKey->render($this);
        }

        return $output;
    }

    /**
     * Renders the indexes.
     * @return string
     */
    public function renderIndexes(): string
    {
        $output = '';

        if ($this->indexes) {
            foreach ($this->indexes as $index) {
                foreach ($this->foreignKeys as $foreignKey) {
                    if ($foreignKey->name === $index->name) {
                        continue 2;
                    }
                }

                $output .= "\n" . $index->render($this);
            }
        }

        return $output;
    }

    /**
     * Renders the foreign keys.
     * @param array $onlyForTables
     * @param array $exceptForTables
     * @return string
     */
    public function renderForeignKeys(array $onlyForTables = [], array $exceptForTables = []): string
    {
        $output = '';

        if (!$this->foreignKeys) {
            return $output;
        }

        foreach ($this->foreignKeys as $foreignKey) {
            if ($onlyForTables && !in_array($foreignKey->refTable, $onlyForTables, true)) {
                continue;
            }
            if ($exceptForTables && in_array($foreignKey->refTable, $exceptForTables, true)) {
                continue;
            }
            $output .= "\n" . $foreignKey->render($this);
        }

        return $output;
    }

    /**
     * Builds table structure based on the list of changes from the Updater.
     * @param TableChange[] $changes
     * @throws InvalidArgumentException
     */
    public function applyChanges(array $changes): void
    {
        /* @var $change TableChange */
        foreach ($changes as $change) {
            if (!$change instanceof TableChange) {
                throw new InvalidArgumentException('You must provide array of TableChange objects.');
            }

            switch ($change->method) {
                case 'createTable':
                    /* @var $column TableColumn */
                    foreach ($change->value as $column) {
                        $this->columns[$column->name] = $column;

                        if ($column->isPrimaryKey || $column->isColumnAppendPK()) {
                            if ($this->primaryKey === null) {
                                $this->primaryKey = new TablePrimaryKey(['columns' => [$column->name]]);
                            } else {
                                $this->primaryKey->addColumn($column->name);
                            }
                        }
                    }
                    break;

                case 'addColumn':
                    $this->columns[$change->value->name] = $change->value;

                    if ($change->value->isPrimaryKey || $change->value->isColumnAppendPK()) {
                        if ($this->primaryKey === null) {
                            $this->primaryKey = new TablePrimaryKey(['columns' => [$change->value->name]]);
                        } else {
                            $this->primaryKey->addColumn($change->value->name);
                        }
                    }
                    break;

                case 'dropColumn':
                    unset($this->columns[$change->value]);
                    break;

                case 'renameColumn':
                    if (isset($this->columns[$change->value['old']])) {
                        $this->columns[$change->value['new']] = $this->columns[$change->value['old']];
                        $this->columns[$change->value['new']]->name = $change->value['new'];

                        unset($this->columns[$change->value['old']]);
                    }
                    break;

                case 'alterColumn':
                    $this->columns[$change->value->name] = $change->value;
                    break;

                case 'addPrimaryKey':
                    $this->primaryKey = $change->value;

                    foreach ($this->primaryKey->columns as $column) {
                        if (isset($this->columns[$column])) {
                            if (empty($this->columns[$column]->append)) {
                                $this->columns[$column]->append = $this->columns[$column]->prepareSchemaAppend(
                                    true,
                                    false
                                );
                            } elseif (!$this->columns[$column]->isColumnAppendPK()) {
                                $this->columns[$column]->append .= ' ' . $this->columns[$column]->prepareSchemaAppend(
                                    true,
                                    false
                                );
                            }
                        }
                    }
                    break;

                case 'dropPrimaryKey':
                    if ($this->primaryKey !== null) {
                        foreach ($this->primaryKey->columns as $column) {
                            if (isset($this->columns[$column]) && !empty($this->columns[$column]->append)) {
                                $this->columns[$column]->append = $this->columns[$column]->removePKAppend();
                            }
                        }
                    }

                    $this->primaryKey = null;
                    break;

                case 'addForeignKey':
                    $this->foreignKeys[$change->value->name] = $change->value;
                    break;

                case 'dropForeignKey':
                    unset($this->foreignKeys[$change->value]);
                    break;

                case 'createIndex':
                    $this->indexes[$change->value->name] = $change->value;
                    if ($change->value->unique
                        && isset($this->columns[$change->value->columns[0]])
                        && count($change->value->columns) === 1
                    ) {
                        $this->columns[$change->value->columns[0]]->isUnique = true;
                    }
                    break;

                case 'dropIndex':
                    if ($this->indexes[$change->value]->unique
                        && count($this->indexes[$change->value]->columns) === 1
                        && isset($this->columns[$this->indexes[$change->value]->columns[0]])
                        && $this->columns[$this->indexes[$change->value]->columns[0]]->isUnique
                    ) {
                        $this->columns[$this->indexes[$change->value]->columns[0]]->isUnique = false;
                    }
                    unset($this->indexes[$change->value]);
                    break;

                case 'addCommentOnColumn':
                    if (isset($this->columns[$change->value->name])) {
                        $this->columns[$change->value->name]->comment = $change->value->comment;
                    }
                    break;

                case 'dropCommentFromColumn':
                    if (isset($this->columns[$change->value])) {
                        $this->columns[$change->value]->comment = null;
                    }
            }
        }
    }
}
