module.exports = function(grunt) {
    grunt.initConfig({
        buildDir: 'dist',
        sassDir: 'css',
        jsDir: 'js',
        sass: {
            dist: {
                files: {
                    '<%= buildDir %>/ladda-themeless.css': '<%= sassDir %>/ladda.scss',
                    '<%= buildDir %>/ladda.css': '<%= sassDir %>/ladda-theme.scss'
                }
            }
        },
        cssmin: {
            combine: {
                files: {
                    '<%= buildDir %>/ladda-themeless.min.css': [ '<%= buildDir %>/ladda-themeless.css' ],
                    '<%= buildDir %>/ladda.min.css': [ '<%= buildDir %>/ladda.css' ]
                }
            }
        },
        uglify: {
            js: {
                options: {
                    mangle: false,
                    beautify: true,
                    compress: false
                },
                files: {
                    '<%= buildDir %>/ladda.js': [ '<%= jsDir %>/ladda.js' ],
                    '<%= buildDir %>/spin.js': [ '<%= jsDir %>/spin.js' ]
                }
            },
            jsmin: {
                options: {
                    mangle: true,
                    compress: true
                },
                files: {
                    '<%= buildDir %>/ladda.min.js': [ '<%= jsDir %>/ladda.js' ],
                    '<%= buildDir %>/spin.min.js': [ '<%= jsDir %>/spin.js' ]
                }
            }
        },
        clean: {
            dist: '<%= buildDir %>'
        }
    });
    grunt.registerTask('default', 'build');
    grunt.registerTask('build', ['sass', 'cssmin', 'uglify']);
    grunt.loadNpmTasks('grunt-contrib-sass');
    grunt.loadNpmTasks('grunt-contrib-cssmin');
    grunt.loadNpmTasks('grunt-contrib-uglify');
    grunt.loadNpmTasks('grunt-contrib-clean');
};
