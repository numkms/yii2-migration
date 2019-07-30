<?php

/**
 * This is the template for generating the migration of a specified table.
 *
 * @var $upContent string Body of up() method.
 * @var $downContent string Body of down() method.
 * @var $name string Migration name
 * @var $namespace string Migration namespace
 */

echo "<?php\n\n";
if ($namespace): ?>
namespace <?= $namespace ?>;
<?php echo "\n"; endif; ?>
use yii\db\Migration;

class <?= $name ?> extends Migration
{
    public function up()
    {
        <?= $upContent ?>
    }

    public function down()
    {
        <?= $downContent ?>
    }
}
