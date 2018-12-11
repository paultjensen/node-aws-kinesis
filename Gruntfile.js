module.exports = function(grunt) {

    // ===========================================================================
    // CONFIGURE GRUNT ===========================================================
    // ===========================================================================
    grunt.initConfig({
        pkg: grunt.file.readJSON('package.json'),

        jshint: {
            options: {
                reporter: require('jshint-stylish')
            },
            build: ['Gruntfile.js',
                'index.js',
                'test/**/*.js'
            ]
        },

        mochaTest: {
            all: {
                options: {
                    reporter: 'spec',
                    quiet: false,
                    clearRequireCache: false
                },
                src: ['test/**/*.js']
            },
            main: {
                options: {
                    reporter: 'spec',
                    quiet: false,
                    clearRequireCache: false
                },
                src: ['test/**/tests.js']
            }
        },

        shell: {
            options: {
                stderr: false
            }
        }

    });

    // ===========================================================================
    // LOAD GRUNT PLUGINS ========================================================
    // ===========================================================================
    require('load-grunt-tasks')(grunt);

    // Default task(s).
    grunt.registerTask('default', ['jshint']);
    grunt.registerTask('inspect', ['jshint']);
    grunt.registerTask('analyze:jshint', ['jshint']);
    grunt.registerTask('test:all', ['mochaTest:all']);

};