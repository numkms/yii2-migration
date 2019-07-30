<?php

declare(strict_types=1);

namespace bizley\migration;

use bizley\migration\table\TableColumnFactory;
use bizley\migration\table\TableForeignKey;
use bizley\migration\table\TableIndex;
use bizley\migration\table\TablePrimaryKey;
use bizley\migration\table\TableStructure;
use Yii;
use yii\base\Component;
use yii\base\InvalidArgumentException;
use yii\base\InvalidConfigException;
use yii\base\View;
use yii\db\Connection;
use yii\db\Constraint;
use yii\db\cubrid\Schema as CubridSchema;
use yii\db\ForeignKeyConstraint;
use yii\db\IndexConstraint;
use yii\db\mssql\Schema as MssqlSchema;
use yii\db\mysql\Schema as MysqlSchema;
use yii\db\oci\Schema as OciSchema;
use yii\db\pgsql\Schema as PgsqlSchema;
use yii\db\sqlite\Schema as SqliteSchema;
use yii\di\Instance;
use yii\helpers\ArrayHelper;
use yii\helpers\FileHelper;
use function array_diff;
use function array_key_exists;
use function count;
use function file_exists;
use function get_class;
use function gmdate;
use function implode;
use function in_array;
use function is_array;
use function is_string;
use function sprintf;
use function strlen;
use function substr;

/**
 * Class NewGenerator
 * @package bizley\migration
 */
class NewGenerator extends Component
{
    /**
     * @var Connection|string|array DB connection object, configuration array, or the application component ID of
     * the DB connection.
     */
    public $db;

    /**
     * @var array Setup of the tables to be generated.
     * Each array element can be either just the string with a table's name (before prefix) or the array with following
     * keys:
     * - name => [string, required] name of the table (before prefix),
     * - prepend => [Closure|string|null] text that should be rendered before migration for the table (default null),
     * - append => [Closure|string|null] text that should be rendered after migration for the table (default null),
     * - tableOptions => [Closure|string|null] text that should appear as rendered third argument of createTable()
     *   method (default "$tableOptions" to match default code in $prepend property of this class, set to null to skip).
     * For closure signatures see the appropriate TableStructure properties.
     * @since 4.0.0
     */
    public $tables = [];

    /**
     * @var View View object responsible for rendering.
     */
    public $view;

    /**
     * @var bool Table prefix flag.
     */
    public $useTablePrefix = true;

    /**
     * @var string Template file - either a path alias (e.g. "@app/migrations/template.php") or a file path.
     * @since 4.0.0
     */
    public $template = '@bizley/migration/views/create_migration.php';

    /**
     * @var string|null Migration namespace.
     */
    public $namespace;

    /**
     * @var bool Whether to use general column schema instead of database specific.
     */
    public $generalSchema = true;

    /**
     * @var string|null Text that should be rendered before all rendered migrations in every generated file.
     * Each rendered createTable() method will use "$tableOptions" as its third argument to match this default value
     * unless it is set to null (or you override it in $table property with [tableOptions] key).
     * @since 4.0.0
     */
    public $prepend = <<<'PHP'
        $tableOptions = null;
        if ($this->db->driverName === 'mysql') {
            $tableOptions = 'CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci ENGINE=InnoDB';
        }
PHP;

    /**
     * @var string|null Text that should be rendered after all rendered migrations.
     * @since 4.0.0
     */
    public $append;

    /**
     * @var bool Whether to generate output as one file or many.
     * @since 4.0.0
     */
    public $allInOne = false;

    /**
     * Checks if DB connection, View, and template path are valid and prepares tables setup.
     * If you extend this component and override the method make sure to call parent::init().
     * @throws InvalidConfigException
     */
    public function init(): void
    {
        parent::init();

        $this->db = Instance::ensure($this->db, Connection::class);
        $this->view = Instance::ensure($this->view, View::class);

        $templatePath = Yii::getAlias($this->template);
        if (!file_exists($templatePath)) {
            throw new InvalidConfigException("Provided template path '$templatePath' does not exist!");
        }

        $this->prepareTableSetup();
    }

    /**
     * @throws InvalidConfigException
     */
    private function prepareTableSetup(): void
    {
        foreach ($this->tables as $table) {
            if (is_array($table)) {
                if (!array_key_exists('name', $table) || empty($table['name'])) {
                    throw new InvalidConfigException("No 'name' value in provided table setup!");
                }

                $prepend = ArrayHelper::getValue($table, 'prepend');
                if ($prepend !== null && !is_string($prepend)) {
                    throw new InvalidConfigException(
                        "Value 'prepend' must be a string or null in provided table setup!"
                    );
                }
                $append = ArrayHelper::getValue($table, 'append');
                if ($append !== null && !is_string($append)) {
                    throw new InvalidConfigException(
                        "Value 'append' must be a string or null in provided table setup!"
                    );
                }
                $tableOptions = ArrayHelper::getValue($table, 'tableOptions', '$tableOptions');
                if ($tableOptions !== null && !is_string($append)) {
                    throw new InvalidConfigException(
                        "Value 'tableOptions' must be a string or null in provided table setup!"
                    );
                }

                $this->tablesSetup[$table['name']] = [
                    'prepend' => $prepend,
                    'append' => $append,
                    'tableOptions' => $tableOptions,
                ];
            } elseif (empty($table)) {
                throw new InvalidConfigException('Empty name in provided table setup!');
            }

            $this->tablesSetup[$table] = [
                'prepend' => null,
                'append' => null,
                'tableOptions' => '$tableOptions',
            ];
        }
    }

    private $tablesSetup = [];

    /**
     * @return array
     * @since 4.0.0
     */
    public function getTablesSetup(): array
    {
        return $this->tablesSetup;
    }

    private $tableStructures = [];

    /**
     * @return array
     * @since 4.0.0
     */
    public function getTableStructures(): array
    {
        return $this->tableStructures;
    }

    /**
     * @param string $table
     * @return TableStructure|null
     * @since 4.0.0
     */
    public function getTableStructure(string $table): ?TableStructure
    {
        return $this->tableStructures[$table] ?? null;
    }

    /**
     * @return array
     * @throws InvalidConfigException
     * @since 4.0.0
     */
    public function generate(): array
    {
        $tableSetup = $this->getTablesSetup();

        if (count($tableSetup) === 0) {
            return [];
        }

        foreach ($tableSetup as $table => $setup) {
            $this->tableStructures[$table] = $this->fetchTableStructure($table, $setup);
        }

        $this->arrangeOrder();

        return $this->renderMigrations();
    }

    /**
     * Returns normalized namespace.
     * @return null|string
     */
    public function getNormalizedNamespace(): ?string
    {
        return !empty($this->namespace) ? FileHelper::normalizePath($this->namespace, '\\') : null;
    }

    /**
     * @return MysqlSchema|OciSchema|MssqlSchema|PgsqlSchema|CubridSchema|SqliteSchema
     */
    public function getDbSchema()
    {
        return $this->db->schema;
    }

    /**
     * @param string $table
     * @param array $setup
     * @return TableStructure
     * @throws InvalidConfigException
     * @since 4.0.0
     */
    public function fetchTableStructure(string $table, array $setup = []): TableStructure
    {
        return new TableStructure([
            'name' => $table,
            'schema' => get_class($this->getDbSchema()),
            'generalSchema' => $this->generalSchema,
            'usePrefix' => $this->useTablePrefix,
            'dbPrefix' => $this->db->tablePrefix,
            'primaryKey' => $this->fetchTablePrimaryKey($table),
            'foreignKeys' => $this->fetchTableForeignKeys($table),
            'indexes' => $this->fetchTableIndexes($table),
            'columns' => $this->fetchTableColumns($table),
            'prepend' => $setup['prepend'] ?? null,
            'append' => $setup['append'] ?? null,
            'tableOptions' => $setup['tableOptions'] ?? null,
        ]);
    }

    private $tablesIndexes = [];

    /**
     * @param string $table
     * @return array indexName => TableIndex
     * @since 4.0.0
     */
    public function fetchTableIndexes(string $table): array
    {
        if (array_key_exists($table, $this->tablesIndexes)) {
            return $this->tablesIndexes[$table];
        }

        $this->tablesIndexes[$table] = [];

        $indexes = $this->getDbSchema()->getTableIndexes($table, true);

        /* @var $index IndexConstraint */
        foreach ($indexes as $index) {
            if (!$index->isPrimary) {
                $this->tablesIndexes[$table][$index->name] = new TableIndex([
                    'name' => $index->name,
                    'unique' => $index->isUnique,
                    'columns' => $index->columnNames,
                ]);
            }
        }

        return $this->tablesIndexes[$table];
    }

    /**
     * @param string $table
     * @return TablePrimaryKey
     * @since 4.0.0
     */
    public function fetchTablePrimaryKey(string $table): TablePrimaryKey
    {
        $data = [];

        /* @var $constraint Constraint */
        $constraint = $this->getDbSchema()->getTablePrimaryKey($table, true);
        if ($constraint) {
            $data = [
                'columns' => $constraint->columnNames,
                'name' => $constraint->name,
            ];
        }

        return new TablePrimaryKey($data);
    }

    /**
     * @param string $table
     * @return array foreignKeyName => TableForeignKey
     * @since 4.0.0
     */
    public function fetchTableForeignKeys(string $table): array
    {
        $data = [];

        $foreignKeys = $this->getDbSchema()->getTableForeignKeys($table, true);

        /* @var $foreignKey ForeignKeyConstraint */
        foreach ($foreignKeys as $foreignKey) {
            $data[$foreignKey->name] = new TableForeignKey([
                'name' => $foreignKey->name,
                'columns' => $foreignKey->columnNames,
                'refTable' => $foreignKey->foreignTableName,
                'refColumns' => $foreignKey->foreignColumnNames,
                'onDelete' => $foreignKey->onDelete,
                'onUpdate' => $foreignKey->onUpdate,
            ]);
        }

        return $data;
    }

    /**
     * @param string $table
     * @return array
     * @throws InvalidConfigException
     * @since 4.0.0
     */
    public function fetchTableColumns(string $table): array
    {
        $columns = [];

        $tableSchema = $this->db->getTableSchema($table);

        if ($tableSchema !== null) {
            $indexes = $this->fetchTableIndexes($table);

            foreach ($tableSchema->columns as $column) {
                $isUnique = false;

                foreach ($indexes as $index) {
                    if ($index->unique && $index->columns[0] === $column->name && count($index->columns) === 1) {
                        $isUnique = true;

                        break;
                    }
                }

                $columns[$column->name] = TableColumnFactory::build([
                    'schema' => TableStructure::identifySchema(get_class($this->getDbSchema())),
                    'name' => $column->name,
                    'type' => $column->type,
                    'size' => $column->size,
                    'precision' => $column->precision,
                    'scale' => $column->scale,
                    'isNotNull' => $column->allowNull ? null : true,
                    'isUnique' => $isUnique,
                    'check' => null, // wywalic?
                    'default' => $column->defaultValue,
                    'isPrimaryKey' => $column->isPrimaryKey,
                    'autoIncrement' => $column->autoIncrement,
                    'isUnsigned' => $column->unsigned,
                    'comment' => $column->comment ?: null,
                ]);
            }
        }

        return $columns;
    }

    private $dependency = [];

    /**
     * @since 4.0.0
     */
    public function getDependency(): array
    {
        if (count($this->dependency) === 0) {
            /* @var $structure TableStructure */
            foreach ($this->getTableStructures() as $table => $structure) {
                if (!array_key_exists($table, $this->dependency)) {
                    $this->dependency[$table] = [];
                }

                foreach ($structure->foreignKeys as $foreignKey) {
                    $this->dependency[$table][] = $foreignKey->refTable;
                }
            }
        }

        return $this->dependency;
    }

    private $mutedDependencies = [];

    /**
     * @param string $table
     * @param string $dependency
     * @since 4.0.0
     */
    public function muteDependency(string $table, string $dependency): void
    {
        if (array_key_exists($table, $this->dependency)) {
            $this->dependency[$table] = array_diff($this->dependency[$table], [$dependency]);

            if (!array_key_exists($table, $this->mutedDependencies)) {
                $this->mutedDependencies[$table] = [];
            }
            if (!in_array($dependency, $this->mutedDependencies, true)) {
                $this->mutedDependencies[$table][] = $dependency;
            }
        }
    }

    /**
     * @return array
     * @since 4.0.0
     */
    public function getTablesMutedDependencies(): array
    {
        return $this->mutedDependencies;
    }

    /**
     * @param string $table
     * @return array|null
     * @since 4.0.0
     */
    public function getTableMutedDependencies(string $table): ?array
    {
        return $this->mutedDependencies[$table] ?? null;
    }

    private $tablesOrder = [];

    /**
     * @return array
     * @since 4.0.0
     */
    public function getTablesOrder(): array
    {
        return $this->tablesOrder;
    }

    /**
     * @since 4.0.0
     */
    public function resetTablesOrder(): void
    {
        $this->tablesOrder = [];
    }

    /**
     * @param string $table
     * @since 4.0.0
     */
    public function addTablesOrder(string $table): void
    {
        $this->tablesOrder[] = $table;
    }

    /**
     * @since 4.0.0
     */
    public function arrangeOrder(): void
    {
        $this->resetTablesOrder();
        $checkList = [];

        $input = $this->getDependency();
        $inputCount = count($input);

        while ($inputCount > count($this->getTablesOrder())) {
            $done = false;
            $lastCheckedName = $lastCheckedDependency = null;

            foreach ($input as $name => $dependencies) {
                if (array_key_exists($name, $checkList)) {
                    continue;
                }

                $resolved = true;

                foreach ($dependencies as $dependency) {
                    if (!array_key_exists($dependency, $checkList)) {
                        $resolved = false;
                        $lastCheckedName = $name;
                        $lastCheckedDependency = $dependency;
                        break;
                    }
                }

                if ($resolved) {
                    $checkList[$name] = true;
                    $this->addTablesOrder($name);

                    $done = true;
                }
            }

            if (!$done) {
                $this->muteDependency($lastCheckedName, (string) $lastCheckedDependency);
                $this->arrangeOrder();
            }
        }
    }

    /**
     * @return array
     * @since 4.0.0
     */
    public function renderMigrations(): array
    {
        $renderedStructures = [];

        foreach ($this->getTablesOrder() as $table) {
            $tableStructure = $this->getTableStructure($table);
            if ($tableStructure === null) {
                throw new InvalidArgumentException("Table '$table' structure has not been found.");
            }

            $renderedStructures[$table] = $tableStructure->render($this->getTableMutedDependencies($table));
        }

        foreach ($this->getTablesMutedDependencies() as $table => $mutedDependencies) {
            $tableStructure = $this->getTableStructure($table);
            if ($tableStructure === null) {
                throw new InvalidArgumentException("Table '$table' structure has not been found.");
            }

            $renderedStructures[$table . '/fk'] = $tableStructure->renderForeignKeys(
                $this->getTableMutedDependencies($table)
            );
        }

        if ($this->allInOne) {
            $migrationName = $this->generateMigrationName('create_tables');

            return [$migrationName => $this->renderFile(
                $this->prepend . implode("\n\n", $renderedStructures) . $this->append,
                $migrationName
            )];
        }

        $batchSize = count($this->getTablesOrder());
        if (count($this->getTablesMutedDependencies())) {
            $batchSize++;
        }

        $migrations = [];
        $counter = 1;
        foreach ($renderedStructures as $table => $renderedStructure) {
            if (substr($table, -3) === '/fk') {
                $migrationName = $this->generateMigrationName('create_foreign_keys', $batchSize, $counter++);
            } else {
                $migrationName = $this->generateMigrationName('create_table_' . $table, $batchSize, $counter++);
            }

            $migrations[$migrationName] = $this->renderFile(
                $this->prepend . $renderedStructure . $this->append,
                $migrationName
            );
        }

        return $migrations;
    }

    /**
     * @param string $subject
     * @param int $batchSize
     * @param int $counter
     * @return string
     * @since 4.0.0
     */
    public function generateMigrationName(string $subject, int $batchSize = 1, int $counter = 1): string
    {
        if ($batchSize > 1) {
            $counterSize = strlen((string)$batchSize);

            return sprintf(
                "m%s_%0{$counterSize}d_%s",
                gmdate('ymd_His'),
                $counter,
                $subject
            );
        }

        return sprintf('m%s_%s', gmdate('ymd_His'), $subject);
    }

    /**
     * @param string $content
     * @param string $name
     * @return string
     * @since 4.0.0
     */
    public function renderFile(string $content, string $name): string
    {
        return $this->view->renderFile(Yii::getAlias($this->template), [
            'content' => $content,
            'name' => $name,
            'namespace' => $this->getNormalizedNamespace(),
        ]);
    }
}
