<?php

declare(strict_types=1);

namespace bizley\migration\controllers;

use bizley\migration\Arranger;
use bizley\migration\Generator;
use bizley\migration\table\TableStructure;
use bizley\migration\Updater;
use Yii;
use yii\base\Action;
use yii\base\ErrorException;
use yii\base\Exception;
use yii\base\InvalidConfigException;
use yii\base\NotSupportedException;
use yii\console\Controller;
use yii\console\controllers\MigrateController;
use yii\console\ExitCode;
use yii\db\Connection;
use yii\db\Exception as DbException;
use yii\di\Instance;
use yii\helpers\Console;
use yii\helpers\FileHelper;
use function array_merge;
use function count;
use function explode;
use function file_put_contents;
use function gmdate;
use function implode;
use function in_array;
use function is_dir;
use function strlen;
use function strpos;
use function time;

/**
 * Migration creator and updater.
 * Generates migration file based on the existing database table and previous migrations.
 *
 * @author Paweł Bizley Brzozowski
 * @version 4.0.0
 * @license Apache 2.0
 * https://github.com/bizley/yii2-migration
 */
class MigrationController extends Controller
{
    /**
     * @var string
     */
    protected $version = '4.0.0';

    /**
     * @var Connection|array|string DB connection object, configuration array, or the application component ID of
     * the DB connection to use when creating migrations.
     */
    public $db = 'db';

    /**
     * @var string Default command action.
     */
    public $defaultAction = 'list';

    /**
     * @var array List of database tables that should be skipped for *-all actions.
     * @since 3.2.0
     */
    public $excludeTables = [];

    /**
     * @var bool Whether to add freshly generated migration to migration history in DB.
     * @since 2.0
     */
    public $fixHistory = false;

    /**
     * @var bool Whether to use general column schema instead of database specific.
     * @since 2.0
     */
    public $generalSchema = true;

    /**
     * @var string Name of the table for keeping applied migration information.
     * The same as in yii\console\controllers\MigrateController::$migrationTable.
     * @since 2.0
     */
    public $migrationTable = '{{%migration}}';

    /**
     * @var string Directory storing the migration classes. This can be either a path alias or a directory.
     */
    public $migrationPath = '@app/migrations';

    /**
     * @var string Full migration namespace. If given it's used instead of $migrationPath. Note that backslash (\)
     * symbol is usually considered a special character in the shell, so you need to escape it properly to avoid shell
     * errors or incorrect behavior.
     * Migration namespace should be resolvable as a path alias if prefixed with @, e.g. if you specify the namespace
     * 'app\migrations', the code Yii::getAlias('@app/migrations') should be able to return the file path to
     * the directory this namespace refers to.
     * @since 1.1
     */
    public $migrationNamespace;

    /**
     * @var bool Whether to only display changes instead of generating update migration.
     * @since 2.0
     */
    public $showOnly = false;

    /**
     * @var array List of migrations from the history table that should be skipped during the update process.
     * Here you can place migrations containing actions that can not be covered by extractor.
     * @since 2.1.1
     */
    public $skipMigrations = [];

    /**
     * @var string|null String rendered in the create migration template for table options.
     * By default it renders "$tableOptions" to indicate that options should be taken from variable
     * set in $tableOptionsInit property.
     * @since 3.0.4
     */
    public $tableOptions = '$tableOptions';

    /**
     * @var string|null String rendered in the create migration template to initialize table options.
     * By default it adds variable "$tableOptions" with optional collate configuration for MySQL DBMS to be used with
     * default $tableOptions.
     * @since 3.0.4
     */
    public $tableOptionsInit = '$tableOptions = null;
        if ($this->db->driverName === \'mysql\') {
            $tableOptions = \'CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci ENGINE=InnoDB\';
        }';

    /**
     * @var array Template files for generating migrations.
     * The array must contain 3 keys:
     * - 'create' - for migrations that create table,
     * - 'update' - for migrations that alter table,
     * - 'foreign_keys' - for migrations that create foreign keys, that can be only applied at the end of process.
     * Each key's value can be either a path alias (e.g. "@app/migrations/template.php") or a file path.
     * @since 4.0.0
     */
    public $templates = [
        'create' => '@bizley/migration/views/create_migration.php',
        'update' => '@bizley/migration/views/update_migration.php',
        'foreign_keys' => '@bizley/migration/views/create_fk_migration.php',
    ];

    /**
     * @var bool Whether the table names generated should consider the $tablePrefix setting of the DB connection.
     * For example, if the table name is 'post' the generator will return '{{%post}}'.
     */
    public $useTablePrefix = true;

    /**
     * {@inheritdoc}
     */
    public function options($actionID) // BC declaration
    {
        $defaultOptions = array_merge(parent::options($actionID), ['db']);

        $createOptions = [
            'migrationPath',
            'migrationNamespace',
            'generalSchema',
            'template',
            'useTablePrefix',
            'fixHistory',
            'migrationTable',
            'tableOptionsInit',
            'tableOptions',
        ];
        $updateOptions = [
            'showOnly',
            'skipMigrations'
        ];

        switch ($actionID) {
            case 'create':
                $options = array_merge($defaultOptions, $createOptions);
                break;

            case 'create-all':
                $options = array_merge(
                    $defaultOptions,
                    $createOptions,
                    ['excludeTables']
                );
                break;

            case 'update':
                $options = array_merge(
                    $defaultOptions,
                    $createOptions,
                    $updateOptions
                );
                break;

            case 'update-all':
                $options = array_merge(
                    $defaultOptions,
                    $createOptions,
                    $updateOptions,
                    ['excludeTables']
                );
                break;

            default:
                $options = $defaultOptions;
        }

        return $options;
    }

    /**
     * {@inheritdoc}
     * @since 2.0
     */
    public function optionAliases(): array
    {
        return array_merge(
            parent::optionAliases(),
            [
                'mp' => 'migrationPath',
                'mn' => 'migrationNamespace',
                'mt' => 'migrationTable',
                'gs' => 'generalSchema',
                'tp' => 'useTablePrefix',
                'fh' => 'fixHistory',
                'so' => 'showOnly',
                'toi' => 'tableOptionsInit',
                'to' => 'tableOptions',
            ]
        );
    }

    protected $workingPath;

    /**
     * This method is invoked right before an action is to be executed (after all possible filters).
     * It checks the existence of the migrationPath and makes sure DB connection is prepared.
     * @param Action $action the action to be executed.
     * @return bool whether the action should continue to be executed.
     * @throws InvalidConfigException
     * @throws Exception
     */
    public function beforeAction($action): bool // BC declaration
    {
        if (!parent::beforeAction($action)) {
            return false;
        }

        if (!$this->showOnly && in_array($action->id, ['create', 'create-all', 'update', 'update-all'], true)) {
            if ($this->migrationPath !== null) {
                $this->migrationPath = $this->preparePathDirectory($this->migrationPath);
            }

            if ($this->migrationNamespace !== null) {
                $this->migrationNamespace = FileHelper::normalizePath($this->migrationNamespace, '\\');
                $this->workingPath = $this->preparePathDirectory(
                    FileHelper::normalizePath('@' . $this->migrationNamespace, '/')
                );
            } else {
                $this->workingPath = $this->migrationPath;
            }
        }

        $this->db = Instance::ensure($this->db, Connection::class);
        $this->stdout("Yii 2 Migration Generator Tool v{$this->version}\n\n", Console::FG_YELLOW, Console::BG_CYAN);

        return true;
    }

    /**
     * Prepares path directory.
     * @param string $path
     * @return string
     * @throws Exception
     * @since 1.1
     */
    public function preparePathDirectory(string $path): string
    {
        $translatedPath = Yii::getAlias($path);

        if (!is_dir($translatedPath)) {
            FileHelper::createDirectory($translatedPath);
        }

        return $translatedPath;
    }

    /**
     * Creates the migration history table.
     * @throws DbException
     * @since 2.0
     */
    protected function createMigrationHistoryTable(): void
    {
        $tableName = $this->db->schema->getRawTableName($this->migrationTable);

        $this->stdout(" > Creating migration history table '{$tableName}' ...", Console::FG_YELLOW);
        $this->db->createCommand()->createTable(
            $this->migrationTable,
            [
                'version' => 'varchar(' . MigrateController::MAX_NAME_LENGTH . ') NOT NULL PRIMARY KEY',
                'apply_time' => 'integer',
            ]
        )->execute();
        $this->db->createCommand()->insert(
            $this->migrationTable,
            [
                'version' => MigrateController::BASE_MIGRATION,
                'apply_time' => time(),
            ]
        )->execute();

        $this->stdout("DONE.\n", Console::FG_GREEN);
    }

    /**
     * Adds migration history entry.
     * @param string $version
     * @param string|null $namespace
     * @throws DbException
     * @since 2.0
     */
    protected function addMigrationHistory(string $version, ?string $namespace = null): void
    {
        $this->stdout(' > Adding migration history entry ...', Console::FG_YELLOW);

        $this->db->createCommand()->insert(
            $this->migrationTable,
            [
                'version' => ($namespace ? $namespace . '\\' : '') . $version,
                'apply_time' => time(),
            ]
        )->execute();

        $this->stdout("DONE.\n", Console::FG_GREEN);
    }

    /**
     * Lists all tables in the database.
     * @return int
     * @since 2.1
     */
    public function actionList(): int
    {
        $tables = $this->db->schema->getTableNames();

        if (!$tables) {
            $this->stdout(" > Your database does not contain any tables yet.\n");
        } else {
            $this->stdout(' > Your database contains ' . count($tables) . " tables:\n");

            foreach ($tables as $table) {
                $this->stdout("   - $table\n");
            }
        }

        $this->stdout("\n > Run\n", Console::FG_GREEN);

        $tab = $this->ansiFormat('<table>', Console::FG_YELLOW);
        $cmd = $this->ansiFormat("{$this->id}/create", Console::FG_CYAN);
        $this->stdout("   $cmd $tab\n");

        $this->stdout("      to generate creating migration for the specific table.\n", Console::FG_GREEN);

        $cmd = $this->ansiFormat("{$this->id}/create-all", Console::FG_CYAN);
        $this->stdout("   $cmd\n");

        $this->stdout("      to generate creating migrations for all the tables.\n", Console::FG_GREEN);

        $cmd = $this->ansiFormat("{$this->id}/update", Console::FG_CYAN);
        $this->stdout("   $cmd $tab\n");

        $this->stdout("      to generate updating migration for the specific table.\n", Console::FG_GREEN);

        $cmd = $this->ansiFormat("{$this->id}/update-all", Console::FG_CYAN);
        $this->stdout("   $cmd\n");

        $this->stdout("      to generate updating migrations for all the tables.\n", Console::FG_GREEN);

        return ExitCode::OK;
    }

    /**
     * @param string $path
     * @param mixed $content
     * @return bool|int
     * @since 3.0.2
     */
    public function generateFile(string $path, $content)
    {
        return file_put_contents($path, $content);
    }

    /**
     * Creates new migration for the given tables.
     * @param string $table Table names separated by commas.
     * @return int
     * @throws DbException
     * @throws InvalidConfigException
     */
    public function actionCreate(string $table): int
    {
        $tables = [$table];
        if (strpos($table, ',') !== false) {
            $tables = explode(',', $table);
        }

        $countTables = count($tables);
        $suppressForeignKeys = [];
        if ($countTables > 1) {
            $arranger = new Arranger([
                'db' => $this->db,
                'inputTables' => $tables,
            ]);
            $arrangedTables = $arranger->arrangeNewMigrations();
            $tables = $arrangedTables['order'];
            $suppressForeignKeys = $arrangedTables['suppressForeignKeys'];

            if (count($suppressForeignKeys)
                && TableStructure::identifySchema(get_class($this->db->schema)) === TableStructure::SCHEMA_SQLITE) {
                $this->stdout(
                    "WARNING!\n > Creating provided tables in batch requires manual migration!\n",
                    Console::FG_RED
                );

                return ExitCode::DATAERR;
            }
        }

        $postponedForeignKeys = [];

        $counterSize = strlen((string)$countTables) + 1;
        $migrationsGenerated = 0;
        foreach ($tables as $name) {
            $this->stdout(" > Generating create migration for table '{$name}' ...", Console::FG_YELLOW);

            if ($countTables > 1) {
                $className = sprintf(
                    "m%s_%0{$counterSize}d_create_table_%s",
                    gmdate('ymd_His'),
                    $migrationsGenerated + 1,
                    $name
                );
            } else {
                $className = sprintf('m%s_create_table_%s', gmdate('ymd_His'), $name);
            }
            $file = Yii::getAlias($this->workingPath . DIRECTORY_SEPARATOR . $className . '.php');

            $generator = new Generator([
                'db' => $this->db,
                'view' => $this->view,
                'useTablePrefix' => $this->useTablePrefix,
                'templateFile' => $this->templates['create'],
                'tableName' => $name,
                'className' => $className,
                'namespace' => $this->migrationNamespace,
                'generalSchema' => $this->generalSchema,
                'tableOptionsInit' => $this->tableOptionsInit,
                'tableOptions' => $this->tableOptions,
                'suppressForeignKey' => $suppressForeignKeys[$name] ?? [],
            ]);

            if ($generator->tableSchema === null) {
                $this->stdout("ERROR!\n > Table '{$name}' does not exist!\n\n", Console::FG_RED);

                return ExitCode::DATAERR;
            }

            if ($this->generateFile($file, $generator->generateMigration()) === false) {
                $this->stdout(
                    "ERROR!\n > Migration file for table '{$name}' can not be generated!\n\n",
                    Console::FG_RED
                );

                return ExitCode::SOFTWARE;
            }

            $migrationsGenerated++;

            $this->stdout("DONE!\n", Console::FG_GREEN);
            $this->stdout(" > Saved as '{$file}'\n");

            if ($this->fixHistory) {
                if ($this->db->schema->getTableSchema($this->migrationTable, true) === null) {
                    $this->createMigrationHistoryTable();
                }

                $this->addMigrationHistory($className, $this->migrationNamespace);
            }

            $this->stdout("\n");

            $postponedForeignKeys = array_merge($postponedForeignKeys, $generator->getSuppressedForeignKeys());
        }

        if ($postponedForeignKeys) {
            $this->stdout(' > Generating create migration for foreign keys ...', Console::FG_YELLOW);

            $className = sprintf(
                "m%s_%0{$counterSize}d_create_foreign_keys",
                gmdate('ymd_His'),
                ++$migrationsGenerated
            );
            $file = Yii::getAlias($this->workingPath . DIRECTORY_SEPARATOR . $className . '.php');

            if ($this->generateFile($file, $this->view->renderFile(Yii::getAlias($this->templateFileForeignKey), [
                'fks' => $postponedForeignKeys,
                'className' => $className,
                'namespace' => $this->migrationNamespace
            ])) === false) {
                $this->stdout(
                    "ERROR!\n > Migration file for foreign keys can not be generated!\n\n",
                    Console::FG_RED
                );

                return ExitCode::SOFTWARE;
            }

            $this->stdout("DONE!\n", Console::FG_GREEN);
            $this->stdout(" > Saved as '{$file}'\n");

            if ($this->fixHistory) {
                $this->addMigrationHistory($className, $this->migrationNamespace);
            }
        }

        if ($migrationsGenerated) {
            $this->stdout(" Generated $migrationsGenerated file(s).\n", Console::FG_YELLOW);
            $this->stdout(" (!) Remember to verify files before applying migration.\n\n", Console::FG_YELLOW);
        } else {
            $this->stdout(" No files generated.\n\n", Console::FG_YELLOW);
        }

        return ExitCode::OK;
    }

    /**
     * Creates new migrations for every table in database.
     * Since 3.0.0 migration history table is skipped.
     * @return int
     * @throws DbException
     * @throws InvalidConfigException
     * @since 2.1
     */
    public function actionCreateAll(): int
    {
        $tables = $this->removeExcludedTables($this->db->schema->getTableNames());

        if (!$tables) {
            $this->stdout(' > Your database does not contain any not excluded tables yet.', Console::FG_YELLOW);

            return ExitCode::OK;
        }

        if ($this->confirm(' > Are you sure you want to generate ' . count($tables) . ' migrations?')) {
            return $this->actionCreate(implode(',', $tables));
        }

        $this->stdout(" Operation cancelled by user.\n\n", Console::FG_YELLOW);

        return ExitCode::OK;
    }

    /**
     * Creates new update migration for the given tables.
     * @param string $table Table names separated by commas.
     * @return int
     * @throws ErrorException
     * @throws DbException
     * @since 2.0
     */
    public function actionUpdate(string $table): int
    {
        $tables = [$table];
        if (strpos($table, ',') !== false) {
            $tables = explode(',', $table);
        }

        $migrationsGenerated = 0;
        foreach ($tables as $name) {
            $this->stdout(" > Generating update migration for table '{$name}' ...", Console::FG_YELLOW);

            $className = 'm' . gmdate('ymd_His') . '_update_table_' . $name;
            $file = Yii::getAlias($this->workingPath . DIRECTORY_SEPARATOR . $className . '.php');

            $updater = new Updater([
                'db' => $this->db,
                'view' => $this->view,
                'useTablePrefix' => $this->useTablePrefix,
                'templateFile' => $this->templates['create'],
                'templateFileUpdate' => $this->templates['update'],
                'tableName' => $name,
                'className' => $className,
                'namespace' => $this->migrationNamespace,
                'migrationPath' => $this->migrationPath,
                'migrationTable' => $this->migrationTable,
                'showOnly' => $this->showOnly,
                'generalSchema' => $this->generalSchema,
                'skipMigrations' => $this->skipMigrations,
            ]);

            if ($updater->tableSchema === null) {
                $this->stdout("ERROR!\n > Table '{$name}' does not exist!\n\n", Console::FG_RED);

                return ExitCode::DATAERR;
            }

            try {
                if (!$updater->isUpdateRequired()) {
                    $this->stdout("UPDATE NOT REQUIRED.\n\n", Console::FG_YELLOW);

                    continue;
                }
            } catch (NotSupportedException $exception) {
                $this->stdout("WARNING!\n > Updating table '{$name}' requires manual migration!\n", Console::FG_RED);
                $this->stdout(' > ' . $exception->getMessage() . "\n\n", Console::FG_RED);

                continue;
            }

            if (!$this->showOnly) {
                if ($this->generateFile($file, $updater->generateMigration()) === false) {
                    $this->stdout(
                        "ERROR!\n > Migration file for table '{$name}' can not be generated!\n\n",
                        Console::FG_RED
                    );

                    return ExitCode::SOFTWARE;
                }

                $migrationsGenerated++;

                $this->stdout("DONE!\n", Console::FG_GREEN);
                $this->stdout(" > Saved as '{$file}'\n");

                if ($this->fixHistory) {
                    if ($this->db->schema->getTableSchema($this->migrationTable, true) === null) {
                        $this->createMigrationHistoryTable();
                    }

                    $this->addMigrationHistory($className, $this->migrationNamespace);
                }
            }

            $this->stdout("\n");
        }

        if ($migrationsGenerated) {
            $this->stdout(" Generated $migrationsGenerated file(s).\n", Console::FG_YELLOW);
            $this->stdout(" (!) Remember to verify files before applying migration.\n\n", Console::FG_YELLOW);
        } else {
            $this->stdout(" No files generated.\n\n", Console::FG_YELLOW);
        }

        return ExitCode::OK;
    }

    /**
     * Creates new update migrations for every table in database.
     * Since 3.0.0 migration history table is skipped.
     * @return int
     * @throws ErrorException
     * @throws DbException
     * @since 2.1
     */
    public function actionUpdateAll(): int
    {
        $tables = $this->removeExcludedTables($this->db->schema->getTableNames());

        if (!$tables) {
            $this->stdout(' > Your database does not contain any not excluded tables yet.', Console::FG_YELLOW);

            return ExitCode::OK;
        }

        if ($this->confirm(' > Are you sure you want to potentially generate ' . count($tables) . ' migrations?')) {
            return $this->actionUpdate(implode(',', $tables));
        }

        $this->stdout(" Operation cancelled by user.\n\n", Console::FG_YELLOW);

        return ExitCode::OK;
    }

    /**
     * Removes excluded tables names from the tables list.
     * @param array $tables
     * @return array
     * @since 3.2.0
     */
    public function removeExcludedTables(array $tables): array
    {
        if (!$tables) {
            return [];
        }

        $filteredTables = [];
        $excludedTables = array_merge(
            [$this->db->schema->getRawTableName($this->migrationTable)],
            $this->excludeTables
        );

        foreach ($tables as $table) {
            if (!in_array($table, $excludedTables, true)) {
                $filteredTables[] = $table;
            }
        }

        return $filteredTables;
    }
}
